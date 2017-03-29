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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.pubsub.proxy.ProxyContext;
import com.google.cloud.pubsub.proxy.PubSub;
import com.google.cloud.pubsub.proxy.message.PublishMessage;
import com.google.cloud.pubsub.proxy.message.SubscribeMessage;
import com.google.cloud.pubsub.proxy.message.UnsubscribeMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * Class to access Google Cloud Pub/Sub instance, publish, subscribe, and unsubscribe to topics.
 */
public final class GcloudPubsub implements PubSub {

  // Google Cloud Pub/Sub specific constants
  /**
   * MQTT Client Id Attribute Key.
   */
  public static final String MQTT_CLIENT_ID = "mqtt_client_id";
  /**
   * MQTT Topic Name Attribute Key.
   */
  public static final String MQTT_TOPIC_NAME = "mqtt_topic_name";
  /**
   * MQTT Message Id Attribute Key.
   */
  public static final String MQTT_MESSAGE_ID = "mqtt_message_id";
  /**
   * Retain Message Attribute Key.
   */
  public static final String MQTT_RETAIN = "mqtt_retain";
  /**
   * Deadline(in seconds) for acknowledging pubsub messages.
   */
  public static final Integer SUBSCRIPTION_ACK_DEADLINE = 600;
  /**
   * Server Id Attribute Key.
   */
  public static final String PROXY_SERVER_ID = "proxy_server_id";
  private static final int RESOURCE_NOT_FOUND = 404;
  private static final int RESOURCE_CONFLICT = 409;
  private static final int MAXIMUM_CPS_TOPIC_LENGTH = 255;
  private static final int MAX_CPS_PAYLOAD_SIZE_BYTES = (int) (9.5 * 1024 * 1024);
  private static final int THREADS = Runtime.getRuntime().availableProcessors();
  private static final String HASH_PREFIX = "sha256-";
  private static final String PREFIX = "cps-";
  private static final String ASTERISK_URLENCODE_VALUE = "%2A";
  // TODO auto detect the project id similar to gcloud(Veneer) libraries
  private static final String GCLOUD_PUBSUB_PROJECT_ID_ENV_VARIABLE = "GCLOUD_PROJECT";
  private static final String CLOUD_PUBSUB_PROJECT_ID =
      System.getenv(GCLOUD_PUBSUB_PROJECT_ID_ENV_VARIABLE);
  private static final String GCLOUD_PUBSUB_PROJECT_ID_NOT_SET_ERROR =
      "Please set the " + GCLOUD_PUBSUB_PROJECT_ID_ENV_VARIABLE
      + " environment variable to your project id";
  private static final String BASE_TOPIC = "projects/"
      + CLOUD_PUBSUB_PROJECT_ID + "/topics/";
  private static final String BASE_SUBSCRIPTION = "projects/"
      + CLOUD_PUBSUB_PROJECT_ID + "/subscriptions/";
  private static final Logger logger = Logger.getLogger(GcloudPubsub.class.getName());
  private final ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(THREADS);
  private final String serverName;
  private Pubsub pubsub; // Google Cloud Pub/Sub instance
  private ProxyContext context;

  /**
   * Constructor that will automatically instantiate a Google Cloud Pub/Sub instance.
   *
   * @throws IOException when the initialization of the Google Cloud Pub/Sub client fails.
   */
  public GcloudPubsub() throws IOException {
    if (CLOUD_PUBSUB_PROJECT_ID == null) {
      throw new IllegalStateException(GCLOUD_PUBSUB_PROJECT_ID_NOT_SET_ERROR);
    }
    try {
      serverName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to retrieve the hostname of the system");
    }
    HttpTransport httpTransport = checkNotNull(Utils.getDefaultTransport());
    JsonFactory jsonFactory = checkNotNull(Utils.getDefaultJsonFactory());
    GoogleCredential credential = GoogleCredential.getApplicationDefault(
        httpTransport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(PubsubScopes.all());
    }
    HttpRequestInitializer initializer =
        new RetryHttpInitializerWrapper(credential);
    pubsub = new Pubsub.Builder(httpTransport, jsonFactory, initializer).build();
    logger.info("Google Cloud Pub/Sub Initialization SUCCESS");
  }

  /**
   * Constructor for using a custom Google Cloud Pub/Sub instance -- could be used for testing.
   *
   * @param pubsub the Google Cloud Pub/Sub instance to use for Pub/Sub operations.
   */
  public GcloudPubsub(Pubsub pubsub) {
    if (CLOUD_PUBSUB_PROJECT_ID == null) {
      throw new IllegalStateException(GCLOUD_PUBSUB_PROJECT_ID_NOT_SET_ERROR);
    }
    try {
      serverName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to retrieve the hostname of the system");
    }
    this.pubsub = checkNotNull(pubsub);
  }

  @Override
  public void initialize(ProxyContext context) {
    this.context = checkNotNull(context);
  }

  /**
   * Publishes a message to Google Cloud Pub/Sub.
   * TODO(rshanky) - Provide config option for automatic topic creation on publish/subscribe through
   * Google Cloud Pub/Sub.
   *
   * @param msg the message and attributes to be published is contained in this object.
   * @throws IOException is thrown on Google Cloud Pub/Sub publish(or createTopic) API failure.
   */
  @Override
  public void publish(PublishMessage msg) throws IOException {
    String publishTopic = createFullGcloudPubsubTopic(msg.getMqttTopic());
    PubsubMessage pubsubMessage = new PubsubMessage();
    byte[] payload = convertMqttPayloadToGcloudPayload(msg.getMqttPaylaod());
    pubsubMessage.setData(new String(payload));
    // create attributes for the message
    Map<String, String> attributes = ImmutableMap.of(MQTT_CLIENT_ID, msg.getMqttClientId(),
        MQTT_TOPIC_NAME, msg.getMqttTopic(),
        MQTT_MESSAGE_ID, msg.getMqttMessageId() == null ? "" : msg.getMqttMessageId().toString(),
        MQTT_RETAIN, msg.isMqttMessageRetained().toString(),
        PROXY_SERVER_ID, serverName);
    pubsubMessage.setAttributes(attributes);
    // publish message
    List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
    PublishRequest publishRequest = new PublishRequest().setMessages(messages);
    try {
      pubsub.projects().topics().publish(publishTopic, publishRequest).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == RESOURCE_NOT_FOUND) {
        logger.info("Cloud PubSub Topic Not Found");
        createTopic(publishTopic);
        pubsub.projects().topics().publish(publishTopic, publishRequest).execute();
      } else {
        // re-throw the exception so that we do not send a PUBACK
        throw e;
      }
    }
    logger.info("Google Cloud Pub/Sub publish SUCCESS for topic " + publishTopic);
  }

  /**
   * Subscribes for a topic through Google Cloud PubSub and updates subscription data structures.
   * TODO avoid unnecessary synchronization for updating data structures and creating subscriptions.
   *
   * @param msg a message that contains information about the topic to subscribe to.
   * @throws IOException is thrown on Google Cloud Pub/Sub subscribe(or createTopic) API failure.
   */
  @Override
  public void subscribe(SubscribeMessage msg) throws IOException {
    String mqttTopic = msg.getMqttTopic();
    String clientId = msg.getClientId();
    // TODO support wildcard subscriptions
    String cpsSubscriptionName =
        createFullGcloudPubsubSubscription(createSubscriptionName(mqttTopic, clientId));
    String cpsSubscriptionTopic = createFullGcloudPubsubTopic(mqttTopic);
    updateOnSubscribe(clientId, mqttTopic, cpsSubscriptionName, cpsSubscriptionTopic);
    logger.info("Cloud PubSub subscribe SUCCESS for topic " + cpsSubscriptionTopic);
  }

  private void subscribe(Subscription subscription, String cpsSubscriptionName,
      String cpsSubscriptionTopic) throws IOException {
    try {
      pubsub.projects().subscriptions().create(cpsSubscriptionName, subscription).execute();
    } catch (GoogleJsonResponseException e) {
      logger.info("Pubsub Subscribe Error code: " + e.getStatusCode() + "\n" + e.getMessage());
      if (e.getStatusCode() == RESOURCE_CONFLICT) {
        // there is already a subscription and a pull request for this topic.
        // do nothing and return.
        // TODO this condition could change based on the implementation of UNSUBSCRIBE
        logger.info("Cloud PubSub subscription already exists");
      } else if (e.getStatusCode() == RESOURCE_NOT_FOUND) {
        logger.info("Cloud PubSub Topic Not Found");
        createTopic(cpsSubscriptionTopic);
        // possible that subscription name already exists, and might throw an exception.
        // But, we should not treat that as an error.
        subscribe(subscription, cpsSubscriptionName, cpsSubscriptionTopic);
      } else {
        // exception was caused due to some other reason, so we re-throw and do not send a SUBACK.
        // client will re-send the subscription.
        throw e;
      }
    }
  }

  /**
   * Updates the subscription data structures by removing entries.
   * TODO avoid any unnecessary synchronization while updating data structures.
   */
  @Override
  public void unsubscribe(UnsubscribeMessage msg) {
    updateOnUnsubscribe(msg.getClientId(), msg.getMqttTopic());
  }

  // updates the subscription data structures by removing entries
  private synchronized void updateOnUnsubscribe(String clientId, String mqttTopic) {
    String subscriptionName = createFullGcloudPubsubSubscription(createSubscriptionName(mqttTopic, clientId));
    try {
      pubsub.projects().subscriptions().delete(subscriptionName);
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == RESOURCE_NOT_FOUND) {
        logger.info("Subscription already deleted");
      }
    } catch (IOException ioe) {
      // we will return false and the pull task will call this method again when rescheduled.
    }

  }

  // synchronized method for updating the subscription maps
  // conditionally creates a pubsub subscription and pull task,
  // if there are no other pull tasks for the pubsub topic
  private synchronized void updateOnSubscribe(String clientId, String mqttTopic,
      String cpsSubscriptionName, String cpsTopic) throws IOException {
    // create pubsub subscription
    Subscription subscription = new Subscription()
        .setTopic(cpsTopic) // the name of the topic
        .setAckDeadlineSeconds(SUBSCRIPTION_ACK_DEADLINE); // acknowledgement deadline in seconds
    subscribe(subscription, cpsSubscriptionName, cpsTopic);
    // schedule pull task for the very first time we have a client Id subscribe to a pubsub topic
    // task must be started after subscription maps are updated
    GcloudPullMessageTask pullTask = new GcloudPullMessageTask.GcloudPullMessageTaskBuilder()
        .withMqttSender(context)
        .withGcloud(this)
        .withPubsub(pubsub)
        .withPubsubExecutor(taskExecutor)
        .withSubscriptionName(cpsSubscriptionName)
        .build();
    taskExecutor.submit(pullTask);
    logger.info("Created Cloud PubSub pulling task for: " + cpsSubscriptionName);
  }

  /**
   * Return the qualified Google Cloud Pub/Sub subscription name for the given MQTT topic.
   *
   * @param mqttTopic the mqtt topic name for this subscription.
   * @param clientId client id unique to user/device
   * @return the Google Cloud Pub/Sub subscription name that will be used for this topic.
   */
  private String createSubscriptionName(String mqttTopic, String clientId) {
    // create subscription name using the format: topic-clientId
    String subscriptionName = clientId + "-" + mqttTopic;
    logger.info("Got client Id" + clientId);
    // if the subscription name exceeds the max length required by pubsub, hash the name
    if (subscriptionName.length() > MAXIMUM_CPS_TOPIC_LENGTH) {
      subscriptionName = HASH_PREFIX + getHashedName(subscriptionName);
    }
    return subscriptionName;
  }

  private String getHashedName(String topic) {
    topic = Hashing.sha256().hashString(topic, StandardCharsets.UTF_8).toString();
    return topic;
  }

  private byte[] convertMqttPayloadToGcloudPayload(byte[] payload) {
    byte[] encodedPayload = BaseEncoding.base64().encode(payload).getBytes();
    // TODO(rshanky) Support sizes bigger than Cloud PubSub max payload size
    // MQTT protocol does not support PUBLISH Failure responses, so we cannot inform
    // client of failure
    checkArgument(encodedPayload.length <= MAX_CPS_PAYLOAD_SIZE_BYTES,
        "Payload size exceeds maximum size of %s bytes", MAX_CPS_PAYLOAD_SIZE_BYTES);
    return encodedPayload;
  }

  private String createFullGcloudPubsubTopic(String topic) {
    return BASE_TOPIC + topic;
  }

  private String createFullGcloudPubsubSubscription(String subscriptionName) {
    return BASE_SUBSCRIPTION + subscriptionName;
  }

  private void createTopic(final String topic) throws IOException {
    try {
      pubsub.projects().topics().create(topic, new Topic()).execute();
    } catch (GoogleJsonResponseException e) {
      // two threads were trying to create topic at the same time
      // first thread created a topic, causing second thread to wait(this method is synchronized)
      // second thread causes an exception since it tries to create an existing topic
      if (e.getStatusCode() == RESOURCE_CONFLICT) {
        logger.info("Topic was created by another thread");
        return ;
      }
      // if it is not a topic conflict(or topic already exists) error,
      // it must be a low level error, and the client should send the PUBLISH packet again for retry
      // we throw the exception, so that we don't send a PUBACK to the client
      throw e;
    }
    logger.info("Google Cloud Pub/Sub Topic Created");
  }

  @Override
  public synchronized void disconnect(String clientId) {
    logger.info("GCloudPubSub disconnecting");
  }

  @Override
  public void destroy() {
    pubsub = null;
    taskExecutor.shutdown();
  }
}
