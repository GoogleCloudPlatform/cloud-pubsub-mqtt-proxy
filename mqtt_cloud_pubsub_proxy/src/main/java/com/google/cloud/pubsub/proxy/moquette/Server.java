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

/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

import static org.eclipse.moquette.commons.Constants.PERSISTENT_STORE_PROPERTY_NAME;

import org.eclipse.moquette.server.ServerAcceptor;
import org.eclipse.moquette.spi.impl.SimpleMessaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

/**
 * Launch a  configured version of the server.
 *
 * <p>Copied from https://github.com/andsel/moquette
 */
public class Server {

  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  private ServerAcceptor acceptor;
  SimpleMessaging messaging;
  Properties properties;

  /**
   * Main method to start the Moquette server.
   * @param args program arguments.
   * @throws IOException throws an IOException when we are unable to start the server
   *and initialize resources.
   */
  public static void main(String[] args) throws IOException {
    final Server server = new Server();
    server.startServer();
    System.out.println("Server started, version 0.8-SNAPSHOT");
    //Bind  a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        server.stopServer();
      }
    });
  }

  /**
   * Starts Moquette bringing the configuration from the file
   * located at config/moquette.conf
   */
  public void startServer() throws IOException {
    String configPath = System.getProperty("moquette.path", null);
    startServer(new File(configPath, "config/moquette.conf"));
  }

  /**
   * Starts Moquette bringing the configuration from the given file.
   */
  public void startServer(File configFile) throws IOException {
    LOG.info("Using config file: " + configFile.getAbsolutePath());

    ConfigurationParser confParser = new ConfigurationParser();
    try {
      confParser.parse(configFile);
    } catch (ParseException pex) {
      LOG.warn("An error occurred in parsing configuration,"
          + " fallback on default configuration", pex);
    }
    properties = confParser.getProperties();
    startServer(properties);
  }

  /**
   * Starts the server with the given properties.
   *
   * <p> Its suggested to at least have the following properties:
   * <ul>
   *  <li>port</li>
   *  <li>password_file</li>
   * </ul>
   */
  public void startServer(Properties configProps) throws IOException {
    ConfigurationParser confParser = new ConfigurationParser(configProps);
    properties = confParser.getProperties();
    LOG.info("Persistent store file: " + properties.get(PERSISTENT_STORE_PROPERTY_NAME));
    messaging = SimpleMessaging.getInstance();
    messaging.init(properties);

    acceptor = new NettyAcceptor();
    acceptor.initialize(messaging, properties);
  }

  /**
   * Terminates the server by closing required resources.
   */
  public void stopServer() {
    LOG.info("Server stopping...");
    messaging.stop();
    acceptor.close();
    LOG.info("Server stopped");
  }

  /**
   * Returns the properties list for the server instance.
   */
  public Properties getProperties() {
    return properties;
  }
}

