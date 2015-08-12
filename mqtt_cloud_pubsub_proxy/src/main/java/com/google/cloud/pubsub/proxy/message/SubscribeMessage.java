package com.google.cloud.pubsub.proxy.message;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for representing a subscribe message that will be sent to some Pub/Sub provider.
 */
public final class SubscribeMessage {

  private final String mqttTopic;
  private final String clientId;

  /**
   * Creates a subscribe message using the given topic name. It is the responsibility of the
   * Pub/Sub provider to convert the mqtt topic to an acceptable format for the Pub/Sub.
   *
   * @param mqttTopic the mqtt topic to subscribe to. It can be a wildcard.
   * @param clientId the client id of the subscriber.
   */
  public SubscribeMessage(String mqttTopic, String clientId) {
    this.clientId = checkNotNull(clientId);
    this.mqttTopic = checkNotNull(mqttTopic);
  }

  /**
   * Returns the mqtt topic to subscribe to.
   */
  public String getMqttTopic() {
    return this.mqttTopic;
  }

  /**
   * Returns the client Id of the subscriber.
   */
  public String getClientId() {
    return this.clientId;
  }
}
