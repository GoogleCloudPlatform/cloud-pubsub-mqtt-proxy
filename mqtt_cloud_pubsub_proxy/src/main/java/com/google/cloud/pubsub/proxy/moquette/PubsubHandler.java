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

package com.google.cloud.pubsub.proxy.moquette;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.eclipse.moquette.proto.messages.AbstractMessage.DISCONNECT;
import static org.eclipse.moquette.proto.messages.AbstractMessage.PUBLISH;
import static org.eclipse.moquette.proto.messages.AbstractMessage.SUBSCRIBE;
import static org.eclipse.moquette.proto.messages.AbstractMessage.UNSUBSCRIBE;

import com.google.cloud.pubsub.proxy.PubSub;
import com.google.cloud.pubsub.proxy.message.PublishMessage;
import com.google.cloud.pubsub.proxy.message.SubscribeMessage;
import com.google.cloud.pubsub.proxy.message.UnsubscribeMessage;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.proto.messages.SubscribeMessage.Couple;
import org.eclipse.moquette.server.netty.NettyChannel;
import org.eclipse.moquette.server.netty.NettyMQTTHandler;
import org.eclipse.moquette.server.netty.NettyUtils;
import org.eclipse.moquette.spi.IMessaging;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class is a handler that invokes the Pubsub operations on receiving a MQTT message
 * and then invokes the MQTT handler.
 */
@Sharable
public class PubsubHandler extends ChannelInboundHandlerAdapter {

  private final NettyMQTTHandler mqttHandler;
  private final PubSub pubsub;
  private static final Logger logger = Logger.getLogger(PubsubHandler.class.getName());

  /**
   * Initializes this handler and sets the MQTT handler.
   *
   * @param handler the MQTT handler instance.
   */
  public PubsubHandler(PubSub pubsub, NettyMQTTHandler mqttHandler) {
    this.mqttHandler = checkNotNull(mqttHandler);
    this.pubsub = checkNotNull(pubsub);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    mqttHandler.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // process the mqtt message using Pubsub provider
    AbstractMessage mqttMsg = (AbstractMessage) msg;
    String clientId = (String) NettyUtils.getAttribute(ctx, NettyChannel.ATTR_KEY_CLIENTID);
    // performing cloud pub/sub publish and subscribe operations before mqtt operation,
    // so that we can use the SUBACK and PUBACK protocol
    // if cloud pub/sub procedures fail, we will not send an ACK,
    // and the MQTT control packet gets resent
    try {
      switch (mqttMsg.getMessageType()) {
        case PUBLISH:
          logger.info("Processing MQTT Publish Control Packet");
          handlePublishMessage(mqttMsg, clientId);
          break;
        case SUBSCRIBE:
          logger.info("Processing MQTT Subscribe Control Packet");
          handleSubscribeMessage(mqttMsg, clientId);
          break;
        default:
          break;
      }
      // process the mqtt message using the original Moquette mqtt handler
      mqttHandler.channelRead(ctx, msg);
      // process unsubscribe and disconnect control packets
      switch (mqttMsg.getMessageType()) {
        case UNSUBSCRIBE:
          logger.info("Processing MQTT Unsubscribe Control Packet");
          handleUnsubscribeMessage(mqttMsg, clientId);
          break;
        case DISCONNECT:
          // TODO unsubscribe from all subscriptions for the specific client id
          break;
        default:
          break;
      }
    } catch (Exception e) {
      // the pubsub provider failed to properly process the message
      // or the mqtt handler failed. The message will be resent and processed again.
      logger.info("An error occured while attempting to process the MQTT message.\n"
          + e.getMessage());
    }
  }

  private void handlePublishMessage(AbstractMessage mqttMsg, String clientId) throws IOException {
    org.eclipse.moquette.proto.messages.PublishMessage mqttPublishMsg =
        (org.eclipse.moquette.proto.messages.PublishMessage) mqttMsg;
    PublishMessage pubsubPublishMsg = new PublishMessage.PublishMessageBuilder()
        .withClientId(clientId)
        .withRetain(mqttMsg.isRetainFlag())
        .withMessageId(mqttPublishMsg.getMessageID())
        .withPayload(mqttPublishMsg.getPayload().array())
        .withTopic(mqttPublishMsg.getTopicName())
        .build();
    pubsub.publish(pubsubPublishMsg);
  }

  private void handleSubscribeMessage(AbstractMessage mqttMsg, String clientId) throws IOException {
    org.eclipse.moquette.proto.messages.SubscribeMessage mqttSubscribeMsg =
        (org.eclipse.moquette.proto.messages.SubscribeMessage) mqttMsg;
    List<Couple> mqttSubscriptions = mqttSubscribeMsg.subscriptions();
    for (Couple mqttSubscription : mqttSubscriptions) {
      String topic = mqttSubscription.getTopicFilter();
      pubsub.subscribe(new SubscribeMessage(topic, clientId));
    }
  }

  private void handleUnsubscribeMessage(AbstractMessage mqttMsg, String clientId) {
    org.eclipse.moquette.proto.messages.UnsubscribeMessage mqttUnsubscribeMsg =
        (org.eclipse.moquette.proto.messages.UnsubscribeMessage) mqttMsg;
    List<String> topics = mqttUnsubscribeMsg.topicFilters();
    for (String topic : topics) {
      pubsub.unsubscribe(new UnsubscribeMessage(topic, clientId));
    }
  }

  /**
   * Sets the callback for handling the MQTT protocol messages.
   *
   * @param messaging the callback instance.
   */
  public void setMessaging(IMessaging messaging) {
    mqttHandler.setMessaging(messaging);
  }
}
