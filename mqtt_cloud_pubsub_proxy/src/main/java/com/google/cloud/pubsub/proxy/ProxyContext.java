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
