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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final int TOPIC_NOT_FOUND_STATUS_CODE = 404;
  private static final int RESOURCE_CONFLICT = 409;
  private static final int MAXIMUM_CPS_TOPIC_LENGTH = 255;
  private static final int MAX_CPS_PAYLOAD_SIZE_BYTES = (int) (9.5 * 1024 * 1024);
  private static final int THREADS = Runtime.getRuntime().availableProcessors();
  private static final String HASH_PREFIX = "sha256-";
  private static final String PREFIX = "cps-";
  private static final String ASTERISK_URLENCODE_VALUE = "%2A";
  // TODO - read project name from config file
  private static final String CLOUD_PUBSUB_PROJECT_NAME = "iot-cloud-pubsub";
  private static final String BASE_TOPIC = "projects/"
      + CLOUD_PUBSUB_PROJECT_NAME + "/topics/";
  private static final String BASE_SUBSCRIPTION = "projects/"
      + CLOUD_PUBSUB_PROJECT_NAME + "/subscriptions/";
  private static final Logger logger = Logger.getLogger(GcloudPubsub.class.getName());
  private final ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(THREADS);
  private Pubsub pubsub; // Google Cloud Pub/Sub instance
  private ProxyContext context;
  private Map<String, Map<String, Set<String>>> clientIdSubscriptionMap = new HashMap<>();
  private Map<String, List<String>> cpsSubscriptionMap = new HashMap<>();

  /**
   * Constructor that will automatically instantiate a Google Cloud Pub/Sub instance.
   *
   * @throws IOException when the initialization of the Google Cloud Pub/Sub client fails.
   */
  public GcloudPubsub() throws IOException {
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
    String publishTopic = createFullGcloudPubsubTopic(createPubSubTopic(msg.getMqttTopic()));
    PubsubMessage pubsubMessage = new PubsubMessage();
    byte[] payload = convertMqttPayloadToGcloudPayload(msg.getMqttPaylaod());
    pubsubMessage.setData(new String(payload));
    // create attributes for the message
    Map<String, String> attributes = ImmutableMap.of(MQTT_CLIENT_ID, msg.getMqttClientId(),
        MQTT_TOPIC_NAME, msg.getMqttTopic(),
        MQTT_MESSAGE_ID, msg.getMqttMessageId().toString(),
        MQTT_RETAIN, msg.isMqttMessageRetained().toString(),
        PROXY_SERVER_ID, InetAddress.getLocalHost().getCanonicalHostName());
    pubsubMessage.setAttributes(attributes);
    // publish message
    List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
    PublishRequest publishRequest = new PublishRequest().setMessages(messages);
    try {
      pubsub.projects().topics().publish(publishTopic, publishRequest).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == TOPIC_NOT_FOUND_STATUS_CODE) {
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
   * Subscribes for a topic through Google Cloud PubSub
   *
   * @param msg a message that contains information about the topic to subscribe to.
   * @throws IOException is thrown on Google Cloud Pub/Sub subscribe(or createTopic) API failure.
   */
  @Override
  public void subscribe(SubscribeMessage msg) throws IOException {
    String mqttTopic = msg.getMqttTopic();
    String clientId = msg.getClientId();
    // TODO support wildcard subscriptions
    String cpsSubscriptionName = createFullGcloudPubsubSubscription(
        createSubcriptionName(mqttTopic));
    String cpsSubscriptionTopic = createFullGcloudPubsubTopic(createPubSubTopic(mqttTopic));
    updateSubscriptionMaps(clientId, mqttTopic, cpsSubscriptionName, cpsSubscriptionTopic);
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
      } else if (e.getStatusCode() == TOPIC_NOT_FOUND_STATUS_CODE) {
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

  // method for updating the map of subscriptions per client Id.
  private void addEntryToClientIdSubscriptionMap(String clientId, String mqttTopic,
      String cpsTopic) {
    Map<String, Set<String>> mqttTopicMap = clientIdSubscriptionMap.get(clientId);
    // create a new map for the very first subscription for a client id
    if (mqttTopicMap == null) {
      mqttTopicMap = new HashMap<>();
      clientIdSubscriptionMap.put(clientId, mqttTopicMap);
      logger.info("First subscription for Client Id: " + clientId);
    }
    // update the list of cps topics for an mqtt topic
    Set<String> cpsTopics = mqttTopicMap.get(mqttTopic);
    if (cpsTopics == null) {
      cpsTopics = new HashSet<>();
      mqttTopicMap.put(mqttTopic, cpsTopics);
    }
    cpsTopics.add(cpsTopic);
  }

  // update cps subscription map
  private void addEntryToCpsSubscriptionMap(String clientId, String cpsTopic,
      String cpsSubscriptionName) {
    List<String> clientIds = cpsSubscriptionMap.get(cpsTopic);
    if (clientIds == null) {
      clientIds = new LinkedList<>();
      cpsSubscriptionMap.put(cpsTopic, clientIds);
    }
    clientIds.add(clientId);
  }

  // synchronized method for updating the subscription maps
  // conditionally creates a pubsub subscription and pull task,
  // if there are no other pull tasks for the pubsub topic
  private synchronized void updateSubscriptionMaps(String clientId, String mqttTopic,
      String cpsSubscriptionName, String cpsTopic) throws IOException {
    List<String> clientIds = cpsSubscriptionMap.get(cpsTopic);
    if (clientIds == null) {
      // create pubsub subscription
      Subscription subscription = new Subscription()
          .setTopic(cpsTopic) // the name of the topic
          .setAckDeadlineSeconds(SUBSCRIPTION_ACK_DEADLINE); // acknowledgement deadline in seconds
      subscribe(subscription, cpsSubscriptionName, cpsTopic);
      // update subscription maps
      addEntryToClientIdSubscriptionMap(clientId, mqttTopic, cpsTopic);
      addEntryToCpsSubscriptionMap(clientId, cpsTopic, cpsSubscriptionName);
      // schedule pull task for the very first time we have a client Id subscribe to a pubsub topic
      // task must be started after subscription maps are updated
      GcloudPullMessageTask pullTask = new GcloudPullMessageTask.GcloudPullMessageTaskBuilder()
          .withMqttSender(context)
          .withPubsub(pubsub)
          .withPubsubExecutor(taskExecutor)
          .withSubscriptionName(cpsSubscriptionName)
          .build();
      taskExecutor.submit(pullTask);
      logger.info("Created Cloud PubSub pulling task for: " + cpsSubscriptionName);
    } else {
      // update subscription maps
      addEntryToClientIdSubscriptionMap(clientId, mqttTopic, cpsTopic);
      addEntryToCpsSubscriptionMap(clientId, cpsTopic, cpsSubscriptionName);
    }
  }

  /**
   * Returns the qualified Google Cloud Pub/Sub topic name for the given MQTT topic by
   * URL encoding and further transforming the MQTT topic name.
   *
   * @param mqttTopic the MQTT topic name.
   * @return the Google Cloud Pub/Sub topic name.
   */
  private String createPubSubTopic(String mqttTopic) {
    // Google Cloud Pub/Sub resource name requirements can be found at https://cloud.google.com/pubsub/overview
    // Adding a prefix to ensure topic name meets minimum length requirement and prevents the
    // topic name from starting with "goog"
    String topic = PREFIX + getEncodedTopicName(mqttTopic);
    // URLEncode to support using special characters in the topic name
    // hash the topic name(sha256 -- 64 character hash) if it exceeds the max length
    if (topic.length() > MAXIMUM_CPS_TOPIC_LENGTH) {
      topic = HASH_PREFIX + getHashedName(topic);
    }
    return topic;
  }

  /**
   * Return the qualified Google Cloud Pub/Sub subscription name for the given MQTT topic.
   *
   * @param mqttTopic the mqtt topic name for this subscription.
   * @return the Google Cloud Pub/Sub subscription name that will be used for this topic.
   */
  private String createSubcriptionName(String mqttTopic) {
    // create subscription name using the format: PREFIX+servername+CP/S-topic-equivalent
    String subscriptionName;
    try {
      subscriptionName = PREFIX + InetAddress.getLocalHost().getCanonicalHostName()
          + getEncodedTopicName(mqttTopic);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to retrieve the hostname of the system");
    }
    // if the subscription name exceeds the max length required by pubsub, hash the name
    if (subscriptionName.length() > MAXIMUM_CPS_TOPIC_LENGTH) {
      subscriptionName = HASH_PREFIX + getHashedName(subscriptionName);
    }
    return subscriptionName;
  }

  private String getEncodedTopicName(String topic) {
    try {
      topic = URLEncoder.encode(topic, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Unable to URL encode the mqtt topic name using "
          + StandardCharsets.UTF_8.name());
    }
    topic = topic.replace("*", ASTERISK_URLENCODE_VALUE);
    return topic;
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
  public void destroy() {
    pubsub = null;
    taskExecutor.shutdown();
    clientIdSubscriptionMap = null;
    cpsSubscriptionMap = null;
  }
}
