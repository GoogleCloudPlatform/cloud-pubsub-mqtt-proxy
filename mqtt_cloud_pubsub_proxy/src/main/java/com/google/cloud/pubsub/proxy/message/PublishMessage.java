package com.google.cloud.pubsub.proxy.message;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for representing a publish message that will be sent to some Pub/Sub client.
 */
public final class PublishMessage {

  private final byte[] mqttPayload;
  private String mqttClientId;
  private final String mqttTopic;
  private Integer mqttMessageId;
  private final Boolean mqttRetain;

  /**
   * Constructor for creating a message the can be utilized by some Pub/Sub provider.
   * The fields in this class are MQTT specific. The underlying Pub/Sub provider should
   * take care of converting fields such as the topic and payload to meet the requirements
   * of the underlying pub/sub client.
   *
   * @param builder the builder instance that will populate the fields in this class.
   */
  private PublishMessage(PublishMessageBuilder builder) {
    this.mqttPayload = checkNotNull(builder.mqttPayload);
    this.mqttRetain = checkNotNull(builder.mqttRetain);
    this.mqttTopic = checkNotNull(builder.mqttTopic);
    // the following variables could be null if the object is being created for an MQTT publish
    this.mqttClientId = builder.mqttClientId;
    this.mqttMessageId = builder.mqttMessageId;
  }

  /**
   * A builder for constructing the message to be published.
   */
  public static final class PublishMessageBuilder {

    private byte[] mqttPayload;
    private String mqttClientId;
    private String mqttTopic;
    private Integer mqttMessageId;
    private Boolean mqttRetain;

    public PublishMessageBuilder withPayload(byte[] payload) {
      this.mqttPayload = payload;
      return this;
    }

    public PublishMessageBuilder withClientId(String clientId) {
      this.mqttClientId = clientId;
      return this;
    }

    public PublishMessageBuilder withTopic(String topic) {
      this.mqttTopic = topic;
      return this;
    }

    public PublishMessageBuilder withRetain(Boolean retain) {
      this.mqttRetain = retain;
      return this;
    }

    public PublishMessageBuilder withMessageId(Integer messageId) {
      this.mqttMessageId = messageId;
      return this;
    }

    public PublishMessage build() {
      return new PublishMessage(this);
    }
  }

  /**
   * Returns the MQTT topic name for this publish message.
   */
  public String getMqttTopic() {
    return this.mqttTopic;
  }

  /**
   * Returns the MQTT payload for this publish message.
   */
  public byte[] getMqttPaylaod() {
    return this.mqttPayload;
  }

  /**
   * Returns the Id of the MQTT client device that sent the MQTT PUBLISH control packet.
   */
  public String getMqttClientId() {
    return this.mqttClientId;
  }
  /**
   * Returns the message Id of the MQTT control packet.
   */
  public Integer getMqttMessageId() {
    return this.mqttMessageId;
  }

  /**
   * Returns true if the last message from the associated client Id should be retained for future
   * subscribers.
   */
  public Boolean isMqttMessageRetained() {
    return this.mqttRetain;
  }
}
