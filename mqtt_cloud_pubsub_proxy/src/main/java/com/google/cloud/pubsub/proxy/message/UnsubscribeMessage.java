package com.google.cloud.pubsub.proxy.message;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for representing an unsubscribe message that will be sent to some Pub/Sub provider.
 */
public final class UnsubscribeMessage {

  private final String mqttTopic;
  private final String clientId;

  public UnsubscribeMessage(String mqttTopic, String clientId) {
    this.clientId = checkNotNull(clientId);
    this.mqttTopic = checkNotNull(mqttTopic);
  }

  /**
   * Returns the mqtt topic to un-subscribe from.
   */
  public String getMqttTopic() {
    return this.mqttTopic;
  }

  /**
   * Returns the Id of the client.
   */
  public String getClientId() {
    return this.clientId;
  }
}
