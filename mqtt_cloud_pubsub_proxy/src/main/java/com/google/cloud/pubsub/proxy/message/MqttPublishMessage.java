package com.google.cloud.pubsub.proxy.message;

import com.google.common.base.Preconditions;

/**
 * Represents a MQTT message that is ready to be published.
 */
public final class MqttPublishMessage {

  private final String topic;
  private final byte[] payload;
  private final boolean retain;

  /**
   * Creates an object that represents an MQTT message that needs to be published to localhost.
   *
   * @param mqttTopic the mqtt topic of the message to be published.
   * @param payload the payload of the message to be published.
   * @param retain whether the published message should be retained for future subscribers.
   */
  public MqttPublishMessage(String mqttTopic, byte[] payload, boolean retain) {
    this.topic = Preconditions.checkNotNull(mqttTopic);
    this.payload = payload;
    this.retain = retain;
  }

  /**
   * Returns the mqtt payload for this message.
   */
  public byte[] getPayload() {
    return payload;
  }

  /**
   * Returns the mqtt topic for this message.
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Returns whether this message should be retained for future subscribers.
   */
  public boolean isRetain() {
    return retain;
  }
}
