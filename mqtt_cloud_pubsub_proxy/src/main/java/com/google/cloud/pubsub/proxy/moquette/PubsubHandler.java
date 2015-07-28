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

import static org.eclipse.moquette.proto.messages.AbstractMessage.PUBLISH;
import static org.eclipse.moquette.proto.messages.AbstractMessage.SUBSCRIBE;
import static org.eclipse.moquette.proto.messages.AbstractMessage.UNSUBSCRIBE;

import com.google.cloud.pubsub.proxy.PubSub;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.netty.NettyMQTTHandler;
import org.eclipse.moquette.spi.IMessaging;

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
    //process the mqtt message using Pubsub provider
    AbstractMessage mqttMsg = (AbstractMessage) msg;
    try {
      switch (mqttMsg.getMessageType()) {
        case PUBLISH:
          //TODO add Pubsub code for publishing message to Pubsub
          break;
        case SUBSCRIBE:
          //TODO add Pubsub code for subscribing
        case UNSUBSCRIBE:
          //TODO add Pubsub code for unsubscribing
        default:
          break;
      }
      //process the mqtt message using the original Moquette mqtt handler
      mqttHandler.channelRead(ctx, msg);
    } catch (Exception e) {
      //the pubsub provider failed to properly process the message
      //or the mqtt handler failed. The message will be resent and processed again.
      logger.info("An error occured while attempting to process the MQTT message.\n"
          + e.getMessage());
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
