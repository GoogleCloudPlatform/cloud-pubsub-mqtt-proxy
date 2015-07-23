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

package com.google.cloud.pubsub.proxy;

import com.google.cloud.pubsub.proxy.message.PublishMessage;
import com.google.cloud.pubsub.proxy.message.SubscribeMessage;

import java.io.IOException;

/**
 * An interface that should be implemented by Publish/Subscribe(Pub/Sub) service providers,
 * such as Google Cloud Pub/Sub. The proxy will act as a bridge between MQTT clients and the
 * implementation of this interface.
 */
public interface PubSub {

  /**
   * Initializes the pubsub provider with a context that should be used for forwarding
   * pubsub messages to MQTT subscribers. This method will be invoked once before
   * using any other methods.
   *
   * @param context the context object that can be used for sending MQTT messages to subscribers.
   *     The context should initialized and ready to send MQTT messages.
   */
  void initialize(ProxyContext context);

  /**
   * Publishes a message using the underlying Pub/Sub implementation.
   *
   * @param msg the message to publish.
   * @throws IOException exception is thrown on Pub/Sub API failure.
   */
  void publish(PublishMessage msg) throws IOException;

  /**
   * Subscribes to the specified topics using the underlying Pub/Sub implementation.
   *
   * @param msg the subscription message which contains the topic to subscribe to.
   * @throws IOException exception is thrown on Pub/Sub API failure.
   */
  void subscribe(SubscribeMessage msg) throws IOException;

  /**
   * Relinquishes the pubsub resources. This method will be invoked when the proxy
   * no longer requires the pubsub instance.
   */
  void destroy();
}
