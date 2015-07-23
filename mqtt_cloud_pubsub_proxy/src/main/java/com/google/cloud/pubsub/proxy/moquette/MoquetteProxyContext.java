package com.google.cloud.pubsub.proxy.moquette;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.pubsub.proxy.ProxyContext;
import com.google.cloud.pubsub.proxy.message.PublishMessage;
import com.google.common.util.concurrent.AbstractFuture;

import org.eclipse.moquette.proto.messages.AbstractMessage.QOSType;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * A class for sending MQTT messages to the Moquette broker running on localhost.
 */
final class MoquetteProxyContext implements ProxyContext {

  private final MqttAsyncClient client;
  private final MqttDefaultFilePersistence dataStore;
  // TODO move these values to a Constants file.
  private static final String MQTT_BROKER_HOST = "127.0.0.1";
  private static final String MQTT_BROKER_PORT = "1883";
  private static final String MQTT_PROTOCOL = "tcp";
  private static final String MQTT_CLIENT_NAME = "localHost-Client";

  /**
   * Initializes an object that can be used for sending messages to the broker
   * which is running on localhost.
   */
  public MoquetteProxyContext() {
    try {
      this.dataStore = new MqttDefaultFilePersistence();
      this.client = new MqttAsyncClient(getFullMqttBrokerUrl(), MQTT_CLIENT_NAME, dataStore);
    } catch (MqttException e) {
      // The exception is thrown when there is an unrecognized MQTT Message in the persistant
      // storage location. Messages are removed from persistant storage once the broker
      // sends the message to subscribers (does not wait for confirmation)
      throw new IllegalStateException("Unrecognized message in the persistent data store location."
          + " Consider clearing the default persistent storage location.");
    }
  }

  /**
   * Initializes an object that can be used for sending messages to the broker
   * which is running on localhost.
   *
   * @param persistenceDir the location of the persistent storage used by the MQTT client library.
   */
  public MoquetteProxyContext(String persistenceDir) {
    try {
      dataStore = new MqttDefaultFilePersistence(checkNotNull(persistenceDir));
      client = new MqttAsyncClient(getFullMqttBrokerUrl(), MQTT_CLIENT_NAME, dataStore);
    } catch (MqttException e) {
      // The exception is thrown when there is an unrecognized MQTT Message in the persistant
      // storage location. Messages are removed from persistant storage once the broker
      // sends the message to subscribers (does not wait for confirmation)
      throw new IllegalStateException("Unrecognized message in the persistent data store location."
          + " Consider clearing the default persistent storage location.");
    }
  }

  /**
   * Connects to the broker running on localhost, so that we can publish messages to the broker.
   *
   * @throws IOException when you're unable to connect to the server.
   */
  public void open() throws IOException {
    // TODO properly handle the exception
    try {
      client.connect();
    } catch (MqttException e) {
      // This exception is thrown if a client is already connected, connection in progress,
      // disconnecting, or client has been closed.
      throw new IOException("The client is in an inappropriate state for connecting."
          + e.getMessage());
    }
  }

  /**
   * Publishes a message to the broker using the Paho client library.
   * You must call open() before using this method.
   *
   * @throws MqttException when the persistent storage location is already in use,
   *     or the client is not in a proper state for publishing a message.
   */
  @Override
  public Future<Boolean> publishToSubscribers(PublishMessage msg) throws IOException {
    // TODO Handle messages that should be retained
    checkNotNull(msg);
    try {
      // We will support QOS Type 1.
      // We must implement a retain datastore, so we don't need to set retain flag for these msgs
      MoquetteMqttListener publishFuture = new MoquetteMqttListener();
      client.publish(msg.getMqttTopic(), msg.getMqttPaylaod(), QOSType.LEAST_ONE.ordinal(),
          false, null, publishFuture);
      return publishFuture;
    } catch (MqttPersistenceException e) {
      // persistent storage location in use
      throw new IOException("The MQTT Persistent Storage Location is already in use. MQTT Reason"
          + " Code: " + e.getReasonCode() + "\n" + e.getMessage());
    } catch (MqttException e) {
      // inappropriate state for publishing
      throw new IOException("The client is in an inappropriate state for connecting."
          + e.getMessage());
    }
  }

  /**
   * Terminates any resources that were used to publish messages to the broker.
   *
   * @throws IOException when the client is in an inappropriate state to disconnect.
   */
  public void close() throws IOException {
    try {
      dataStore.close();
      client.disconnect();
      client.close();
    } catch (MqttException e) {
      // This exception is thrown if a client is not in an appropriate state for disconnecting.
      throw new IOException("The client is in an innapropriate state for disconnecting."
          + e.getMessage());
    }
  }

  private String getFullMqttBrokerUrl() {
    return MQTT_PROTOCOL + "://" + MQTT_BROKER_HOST + ":" + MQTT_BROKER_PORT;
  }

  /**
   * Callback class used for MQTT message operations.
   */
  private final class MoquetteMqttListener extends AbstractFuture<Boolean>
      implements IMqttActionListener {

    @Override
    public void onSuccess(IMqttToken asyncActionToken) {
      set(true);
    }

    @Override
    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
      set(false);
    }
  }
}
