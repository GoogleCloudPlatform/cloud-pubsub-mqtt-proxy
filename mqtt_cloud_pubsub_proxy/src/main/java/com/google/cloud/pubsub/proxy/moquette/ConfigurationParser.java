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

import org.eclipse.moquette.commons.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Mosquitto configuration parser.
 *
 * <p>A line that at the very first has # is a comment
 * Each line has key value format, where the separator used it the space.
 *
 * <p>Copied from https://github.com/andsel/moquette
 */
public class ConfigurationParser {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationParser.class);

  private Properties properties = new Properties();

  ConfigurationParser() {
    createDefaults();
  }

  /**
   * Create a ConfigurationParser merging the default properties with the provided ones.
   */
  ConfigurationParser(Properties properties) {
    this();
    for (Entry<Object, Object> entrySet : properties.entrySet()) {
      properties.put(entrySet.getKey(), entrySet.getValue());
    }
  }

  private void createDefaults() {
    properties.put(Constants.PORT_PROPERTY_NAME, Integer.toString(Constants.PORT));
    properties.put(Constants.HOST_PROPERTY_NAME, Constants.HOST);
    properties.put(Constants.WEB_SOCKET_PORT_PROPERTY_NAME, Integer.toString(
        Constants.WEBSOCKET_PORT));
    properties.put(Constants.PASSWORD_FILE_PROPERTY_NAME, "");
    properties.put(Constants.PERSISTENT_STORE_PROPERTY_NAME, Constants.DEFAULT_PERSISTENT_PATH);
    properties.put(Constants.ALLOW_ANONYMOUS_PROPERTY_NAME, true);
  }

  /**
   * Parse the configuration from file.
   */
  void parse(File file) throws ParseException {
    if (file == null) {
      LOG.warn("parsing NULL file, so fallback on default configuration!");
      return;
    }
    if (!file.exists()) {
      LOG.warn(String.format("parsing not existing file %s, so fallback on default configuration!",
          file.getAbsolutePath()));
      return;
    }
    try {
      FileReader reader = new FileReader(file);
      parse(reader);
    } catch (FileNotFoundException fex) {
      LOG.warn(String.format("parsing not existing file %s, so fallback on default configuration!",
          file.getAbsolutePath()), fex);
      return;
    }
  }

  /**
   * Parse the configuration
   *
   * @throws ParseException if the format is not compliant.
   */
  void parse(Reader reader) throws ParseException {
    if (reader == null) {
      //just log and return default properties
      LOG.warn("parsing NULL reader, so fallback on default configuration!");
      return;
    }

    BufferedReader br = new BufferedReader(reader);
    String line;
    try {
      while ((line = br.readLine()) != null) {
        int commentMarker = line.indexOf('#');
        if (commentMarker != -1) {
          if (commentMarker == 0) {
            //skip its a comment
            continue;
          } else {
            //it's a malformed comment
            throw new ParseException(line, commentMarker);
          }
        } else {
          if (line.isEmpty() || line.matches("^\\s*$")) {
            //skip it's a black line
            continue;
          }
          //split till the first space
          int deilimiterIdx = line.indexOf(' ');
          String key = line.substring(0, deilimiterIdx).trim();
          String value = line.substring(deilimiterIdx).trim();

          properties.put(key, value);
        }
      }
    } catch (IOException ex) {
      throw new ParseException("Failed to read", 1);
    }
  }

  /**
   * Returns the properties for this Configuration parser.
   */
  Properties getProperties() {
    return properties;
  }
}
