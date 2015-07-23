package com.google.cloud.pubsub.proxy;

import com.google.cloud.pubsub.proxy.message.PublishMessage;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * An interface for allowing a PubSub provider to interact with the MQTT proxy including
 * forwarding messages to its subscribed clients.
 */
public interface ProxyContext {

  /**
   * Forwards the message received from the Pub/Sub provider to the interested MQTT subscribers.
   *
   * @param msg the message that should be forwarded to interested MQTT subscribers.
   * @return a future which can be used to retrieve the delivery status of the message.
   *     Returns true on successful delivery and false on failure.
   * @throws IOException when the underlying client is unable to send the message.
   */
  Future<Boolean> publishToSubscribers(PublishMessage msg) throws IOException;
}
