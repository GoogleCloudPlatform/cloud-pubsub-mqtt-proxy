/*
 * Copyright 2015 Google Inc, Andrea Selva. All Rights Reserved.
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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Timeout Handler class for the Moquette Server.
 */
public class MoquetteIdleTimeoutHandler extends ChannelDuplexHandler {
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleState state = ((IdleStateEvent) evt).state();
      if (state == IdleState.ALL_IDLE) {
        //fire a channelInactive to trigger publish of Will
        ctx.fireChannelInactive();
        ctx.close();
      } /*else if (e.getState() == IdleState.WRITER_IDLE) {
          ctx.writeAndFlush(new PingMessage());
      }*/
    }
  }
}