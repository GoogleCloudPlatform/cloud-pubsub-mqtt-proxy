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
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.pubsub.proxy.PubSub;
import com.google.cloud.pubsub.proxy.RetryHttpInitializerWrapper;
import com.google.cloud.pubsub.proxy.message.PubSubPublishMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Class to access Google Cloud Pub/Sub instance, publish, subscribe, and unsubscribe to topics.
 */
public final class GcloudPubsub implements PubSub {

  //Google Cloud Pub/Sub specific constants
  private static final int TOPIC_NOT_FOUND_STATUS_CODE = 404;
  private static final int TOPIC_CONFLICT = 409;
  private static final int MAXIMUM_CPS_TOPIC_LENGTH = 255;
  private static final int MAX_CPS_PAYLOAD_SIZE_BYTES = (int) (9.5 * 1024 * 1024);
  private static final String ILLEGAL_TOPIC_PREFIX = "goog";
  private static final String PREFIX = "cps-";
  private static final String ASTERISK_URLENCODE_VALUE = "%2A";
  //TODO - read project name from config file
  private static final String CLOUD_PUBSUB_PROJECT_NAME = "iot-cloud-pubsub";
  private static final String BASE_TOPIC = "projects/"
      + CLOUD_PUBSUB_PROJECT_NAME + "/topics/";
  private static final Logger logger = Logger.getLogger(GcloudPubsub.class.getName());
  //Google Cloud Pub/Sub instance
  private final Pubsub pubsub;

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
    this.pubsub = pubsub;
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
  public void publish(PubSubPublishMessage msg) throws IOException {
    String publishTopic = createFullGcloudPubsubTopic(getPubSubTopic(msg.getMqttTopic()));
    PubsubMessage pubsubMessage = new PubsubMessage();
    byte[] payload = createGcloudPubsubPayload(msg.getMqttPaylaod());
    pubsubMessage.setData(new String(payload));
    //create attributes for the message
    Map<String, String> attributes = ImmutableMap.of(MQTT_CLIENT_ID, msg.getMqttClientId(),
        MQTT_TOPIC_NAME, msg.getMqttTopic(),
        MQTT_MESSAGE_ID, msg.getMqttMessageId().toString(),
        MQTT_RETAIN, msg.isMqttMessageRetained().toString());
    pubsubMessage.setAttributes(attributes);
    //publish message
    List<PubsubMessage> messages = ImmutableList.of(pubsubMessage);
    PublishRequest publishRequest = new PublishRequest().setMessages(messages);
    try {
      pubsub.projects().topics().publish(publishTopic, publishRequest).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == TOPIC_NOT_FOUND_STATUS_CODE) {
        logger.info("Cloud PubSub Topic Not Found");
        createTopic(publishTopic);
        pubsub.projects().topics().publish(publishTopic, publishRequest).execute();
      }
    }
    logger.info("Google Cloud Pub/Sub publish SUCCESS for topic " + publishTopic);
  }

  /**
   * Returns the qualified Google Cloud Pub/Sub topic name for the given MQTT topic by
   * URL encoding and further transforming the MQTT topic name.
   *
   * @param mqttTopic the MQTT topic name.
   * @return the Google Cloud Pub/Sub topic name.
   */
  private String getPubSubTopic(String mqttTopic) {
    // TODO Auto-generated method stub
    //Google Cloud Pub/Sub resource name requirements can be found at
    //https://cloud.google.com/pubsub/overview
    //ensure topic name meets minimum length requirement for CPS
    String topic = PREFIX + mqttTopic;
    //URLEncode to support using special characters in the topic name
    try {
      topic = URLEncoder.encode(topic, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      throw new IllegalStateException("Unable to URL encode the mqtt topic name using "
          + StandardCharsets.UTF_8.name());
    }
    topic = topic.replace("*", ASTERISK_URLENCODE_VALUE);
    //ensure encoded topic name doesn't exceed max cps topic length
    //encoding will not replace "goog" if the topic starts with it.
    //So, checking after encoding is safe.
    checkArgument(!topic.startsWith(ILLEGAL_TOPIC_PREFIX), "Topic names cannot start with \"%s\"",
        ILLEGAL_TOPIC_PREFIX);
    //TODO(rshanky) Support topic names longer than MAX CPS Topic size
    checkArgument(topic.length() <= MAXIMUM_CPS_TOPIC_LENGTH, "The provided topic is too long."
        + " Please use a topic name that is less than %s characters.", MAXIMUM_CPS_TOPIC_LENGTH);
    return topic;
  }

  private byte[] createGcloudPubsubPayload(byte[] payload) {
    byte[] encodedPayload = BaseEncoding.base64().encode(payload).getBytes();
    //TODO(rshanky) Support sizes bigger than Cloud PubSub max payload size
    //MQTT protocol does not support PUBLISH Failure responses, so we cannot inform
    //client of failure
    checkArgument(encodedPayload.length <= MAX_CPS_PAYLOAD_SIZE_BYTES,
        "Payload size exceeds maximum size of %s bytes", MAX_CPS_PAYLOAD_SIZE_BYTES);
    return encodedPayload;
  }

  private String createFullGcloudPubsubTopic(String topic) {
    return BASE_TOPIC + topic;
  }

  private void createTopic(final String topic) throws IOException {
    try {
      pubsub.projects().topics().create(topic, new Topic()).execute();
    } catch (GoogleJsonResponseException e) {
      //two threads were trying to create topic at the same time
      //first thread created a topic, causing second thread to wait(this method is synchronized)
      //second thread causes an exception since it tries to create an existing topic
      if (e.getStatusCode() == TOPIC_CONFLICT) {
        logger.info("Topic was created by another thread");
        return ;
      }
      //if it is not a topic conflict(or topic already exists) error,
      //it must be a low level error, and the client should send the PUBLISH packet again for retry
      //we throw the exception, so that we don't send a PUBACK to the client
      throw e;
    }
    logger.info("Google Cloud Pub/Sub Topic Created");
  }
}
