/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub.proxy.gcloud;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.cloud.pubsub.proxy.ProxyContext;
import com.google.cloud.pubsub.proxy.message.PublishMessage;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Class for creating/rescheduling a pull message task through Google Cloud Pub/Sub.
 */
final class GcloudPullMessageTask implements Callable<Void> {

  private static int BATCH_SIZE = 100;
  private static int ACK_CHECK_DELAY = 5;
  private static int PUBSUB_RPC_FAILURE_DELAY = 5;
  private static int MAX_PULL_DELAY = 10 * 60; // 10 minutes
  private static final Logger logger = Logger.getLogger(GcloudPullMessageTask.class.getName());
  // task members
  private final Pubsub pubsub;
  private final String subscriptionName;
  private final ProxyContext mqttSender;
  private final ScheduledExecutorService pubsubExecutor;
  private final int delayTime;

  private GcloudPullMessageTask(GcloudPullMessageTaskBuilder builder) {
    this.pubsub = checkNotNull(builder.pubsub);
    this.subscriptionName = checkNotNull(builder.subscriptionName);
    this.pubsubExecutor = checkNotNull(builder.pubsubExecutor);
    this.delayTime = builder.delayTime;
    this.mqttSender = checkNotNull(builder.context);
  }

  /**
   * Builder for constructing the pull message task.
   */
  static final class GcloudPullMessageTaskBuilder {

    private Pubsub pubsub;
    private String subscriptionName;
    private ProxyContext context;
    private ScheduledExecutorService pubsubExecutor;
    private int delayTime;

    public GcloudPullMessageTaskBuilder withPubsub(Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    public GcloudPullMessageTaskBuilder withSubscriptionName(String subscriptionName) {
      this.subscriptionName = subscriptionName;
      return this;
    }

    public GcloudPullMessageTaskBuilder withMqttSender(ProxyContext mqttSender) {
      this.context = mqttSender;
      return this;
    }

    public GcloudPullMessageTaskBuilder withPubsubExecutor(
        ScheduledExecutorService pubsubExecutor) {
      this.pubsubExecutor = pubsubExecutor;
      return this;
    }

    public GcloudPullMessageTaskBuilder withTaskDelayTime(int delayTime) {
      this.delayTime = delayTime;
      return this;
    }

    public GcloudPullMessageTask build() {
      return new GcloudPullMessageTask(this);
    }
  }

  @Override
  public Void call() {
    int pullDelayTime;
    List<ReceivedMessage> msgs = null;
    try {
      msgs = getMessagesFromPubsub(subscriptionName);
    } catch (IOException e) {
      // pubsub API failure, so exponentially backoff(bounded)
      pullDelayTime = Math.min(Math.max(delayTime, 1) * PUBSUB_RPC_FAILURE_DELAY, MAX_PULL_DELAY);
      GcloudPullMessageTask pullMessageTask = toBuilder()
          .withTaskDelayTime(pullDelayTime)
          .build();
      pubsubExecutor.schedule(pullMessageTask, pullDelayTime, TimeUnit.SECONDS);
      return null;
    }
    // handle the received messages
    if (msgs != null) {
      // reset delayTime -- we might have more messages arriving soon
      pullDelayTime = 0;
      logger.info("Received Messages from Pubsub");
      // start the stopwatch to measure how long since we pulled the message
      long ackDeadlineTimer = System.currentTimeMillis();
      for (ReceivedMessage msg : msgs) {
        // async mqtt publish and then create task for pubsub ack
        handlePubsubMessage(msg, ackDeadlineTimer);
      }
    } else { // set longer delay since we haven't received any new messages
      pullDelayTime = ACK_CHECK_DELAY;
    }
    // TODO limit number of messages that are in the process of being acked -- avoid large queues
    GcloudPullMessageTask pullMessageTask = toBuilder()
        .withTaskDelayTime(pullDelayTime)
        .build();
    pubsubExecutor.schedule(pullMessageTask, pullDelayTime, TimeUnit.SECONDS);
    return null;
  }

  /**
   * Attempts to send the received pubsub message to localhost as an MQTT message.
   *
   * @param msg the message received from cloud pubsub.
   */
  private void handlePubsubMessage(ReceivedMessage msg, long ackDeadlineTimer) {
    Future<Boolean> checkStatusTask;
    logger.info("Sending MQTT message to localhost");
    try {
      checkStatusTask = mqttSender.publishToSubscribers(createMqttPublishMessage(msg));
    } catch (IOException e) {
      // failed to send the message. do not ack the pubsub message and wait for re-delivery.
      logger.info("MQTT send error: " + e.getMessage());
      return;
    }
    // create a task for renewing the pubsub message if needed
    GcloudRenewMessageTask renewTask = new GcloudRenewMessageTask.GcloudRenewMessageTaskBuilder()
        .withAckDeadlineTimer(ackDeadlineTimer)
        .withAckId(msg.getAckId())
        .withFutureTask(checkStatusTask)
        .withPubsub(pubsub)
        .withPubsubExecutor(pubsubExecutor)
        .withSubscriptionName(subscriptionName)
        .withTaskDelayTime(ACK_CHECK_DELAY)
        .build();
    pubsubExecutor.schedule(renewTask, ACK_CHECK_DELAY, TimeUnit.SECONDS);
  }

  private GcloudPullMessageTaskBuilder toBuilder() {
    return new GcloudPullMessageTask.GcloudPullMessageTaskBuilder()
        .withMqttSender(mqttSender)
        .withPubsub(pubsub)
        .withPubsubExecutor(pubsubExecutor)
        .withSubscriptionName(subscriptionName)
        .withTaskDelayTime(delayTime);
  }

  private PublishMessage createMqttPublishMessage(ReceivedMessage msg) {
    byte [] mqttPayload = BaseEncoding.base64().decode(msg.getMessage().getData());
    Map<String, String> mqttAttributes = msg.getMessage().getAttributes();
    PublishMessage mqttMsg = new PublishMessage.PublishMessageBuilder()
        .withPayload(mqttPayload)
        .withTopic(mqttAttributes.get(GcloudPubsub.MQTT_TOPIC_NAME))
        .withRetain(Boolean.valueOf(mqttAttributes.get(GcloudPubsub.MQTT_RETAIN)))
        .build();
    return mqttMsg;
  }

  private List<ReceivedMessage> getMessagesFromPubsub(String subscriptionName) throws IOException {
    PullRequest request = new PullRequest().setReturnImmediately(true).setMaxMessages(BATCH_SIZE);
    List<ReceivedMessage> msgs = pubsub.projects().subscriptions().pull(subscriptionName, request)
        .execute().getReceivedMessages();
    if (msgs == null) {
      return null;
    }
    // separate received messages into those that were published to pubsub from localhost
    // and those published from other servers
    List<ReceivedMessage> nonLocalMsgs = new LinkedList<ReceivedMessage>();
    List<String> localMessageAckIds = new LinkedList<>();
    for (ReceivedMessage msg : msgs) {
      String msgServerId = msg.getMessage().getAttributes().get(GcloudPubsub.PROXY_SERVER_ID);
      String serverId = InetAddress.getLocalHost().getCanonicalHostName();
      if (msgServerId.equals(serverId)) {
        logger.info("Discarding Pubsub message -- Message was published to pubsub from localhost");
        localMessageAckIds.add(msg.getAckId());
      } else {
        nonLocalMsgs.add(msg);
      }
    }
    if (!localMessageAckIds.isEmpty()) {
      ackPubsubMessage(localMessageAckIds);
    }
    return nonLocalMsgs;
  }

  private void ackPubsubMessage(List<String> ackIds) {
    AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
    try {
      pubsub.projects().subscriptions().acknowledge(subscriptionName, ackRequest).execute();
      logger.info("Successfully Acked Pubsub message");
    } catch (IOException e) {
      // we are unable to ack the message and it will get re-delivered(as a duplicate message).
      // since, we are only supporting QOS 1 for MQTT this is fine.
      logger.info("Unable to Ack Pubsub message");
    }
  }
}
