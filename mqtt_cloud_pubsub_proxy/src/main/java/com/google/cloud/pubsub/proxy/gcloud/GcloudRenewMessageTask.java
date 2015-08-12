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
import com.google.api.services.pubsub.model.ModifyAckDeadlineRequest;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * A class for periodically checking the expiration and conditionally acknowledging a Google Cloud
 * Pub/Sub message.
 */
class GcloudRenewMessageTask implements Callable<Void> {

  private static final Logger logger = Logger.getLogger(GcloudRenewMessageTask.class.getName());
  private final Future<Boolean> msgStatusTask;
  private final int delayTime;
  private final ScheduledExecutorService pubsubExecutor;
  private final String subscriptionName;
  private final Pubsub pubsub;
  private final String ackId;
  private final long ackDeadlineTimer;

  private GcloudRenewMessageTask(GcloudRenewMessageTaskBuilder builder) {
    this.pubsub = checkNotNull(builder.pubsub);
    this.msgStatusTask = checkNotNull(builder.msgStatusTask);
    this.ackDeadlineTimer = builder.ackDeadlineTimer;
    this.pubsubExecutor = checkNotNull(builder.pubsubExecutor);
    this.subscriptionName = checkNotNull(builder.subscriptionName);
    this.ackId = checkNotNull(builder.ackId);
    this.delayTime = builder.delayTime;
  }

  static final class GcloudRenewMessageTaskBuilder {

    private Pubsub pubsub;
    private String subscriptionName;
    private ScheduledExecutorService pubsubExecutor;
    private long ackDeadlineTimer;
    private String ackId;
    private Future<Boolean> msgStatusTask;
    private int delayTime;

    public GcloudRenewMessageTaskBuilder withPubsub(Pubsub pubsub) {
      this.pubsub = pubsub;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withSubscriptionName(String subscriptionName) {
      this.subscriptionName = subscriptionName;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withPubsubExecutor(
        ScheduledExecutorService pubsubExecutor) {
      this.pubsubExecutor = pubsubExecutor;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withAckDeadlineTimer(long ackDeadlineTimer) {
      this.ackDeadlineTimer = ackDeadlineTimer;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withAckId(String ackId) {
      this.ackId = ackId;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withFutureTask(Future<Boolean> msgStatusTask) {
      this.msgStatusTask = msgStatusTask;
      return this;
    }

    public GcloudRenewMessageTaskBuilder withTaskDelayTime(int delayTime) {
      this.delayTime = delayTime;
      return this;
    }

    public GcloudRenewMessageTask build() {
      return new GcloudRenewMessageTask(this);
    }
  }

  @Override
  public Void call() {
    if (!msgStatusTask.isDone()) {
      //attempt to reschedule this task
      attemptReschedule();
    } else {
      try {
        boolean res = msgStatusTask.get();
        if (res) {
          logger.info("MQTT Publish SUCCESS. Acking Pubsub Message");
          ackPubsubMessage(subscriptionName);
        } else {
          // do not ack the message -- we will wait for re-delivery
          logger.info("MQTT Publish Failed.");
        }
      } catch (InterruptedException | ExecutionException e) {
        // do not ack the message and wait for re-delivery
        logger.info("An error occured while retreiving the future task result.\n"
            + e.getCause().getMessage());
      }
    }
    return null;
  }

  /**
   * Attempts to reschedule the task. Reschedule fails if we need to extend the ack deadline,
   * but are unable due to a pubsub API failure.
   */
  private void attemptReschedule() {
    long deadlineTimer = ackDeadlineTimer;
    if (elapsedTime() + delayTime
        >= GcloudPubsub.SUBSCRIPTION_ACK_DEADLINE) {
      List<String> ackIds = ImmutableList.of(ackId);
      ModifyAckDeadlineRequest ackPostpone = new ModifyAckDeadlineRequest();
      ackPostpone.setAckIds(ackIds);
      ackPostpone.setAckDeadlineSeconds(GcloudPubsub.SUBSCRIPTION_ACK_DEADLINE);
      try {
        pubsub.projects().subscriptions().modifyAckDeadline(subscriptionName, ackPostpone)
          .execute();
        // we need to reset the watch
        // we need to create a new stopwatch because other renew message tasks that share the same
        // subscription name are using the same stopwatch instance.
        deadlineTimer = System.currentTimeMillis();
        logger.info("Extended Ack deadline");
      } catch (IOException e) {
        logger.info("Unable to extend ack deadline. Giving up task and waiting for re-delivery.");
        return ;
      }
    }
    GcloudRenewMessageTask renewMsgTask = toBuilder()
        .withAckDeadlineTimer(deadlineTimer)
        .build();
    pubsubExecutor.schedule(renewMsgTask, delayTime, TimeUnit.SECONDS);
  }

  private GcloudRenewMessageTaskBuilder toBuilder() {
    return new GcloudRenewMessageTask.GcloudRenewMessageTaskBuilder()
        .withAckDeadlineTimer(ackDeadlineTimer)
        .withAckId(ackId)
        .withFutureTask(msgStatusTask)
        .withPubsub(pubsub)
        .withPubsubExecutor(pubsubExecutor)
        .withSubscriptionName(subscriptionName)
        .withTaskDelayTime(delayTime);
  }

  /**
   * Returns the time elapsed from when the associated pubsub message was received or
   * from when its ack deadline was extended.
   */
  private int elapsedTime() {
    return (int)((System.currentTimeMillis() - ackDeadlineTimer) / 1000);
  }

  private void ackPubsubMessage(String subscriptionName) {
    List<String> ackIds = ImmutableList.of(ackId);
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
