## What is MQTT Cloud Pub/Sub Proxy?

The MQTT Cloud Pub/Sub proxy provides a solution for utilizing Cloud Pub/Sub with IoT client
devices through an MQTT interface.

[MQTT][1] is a lightweight protocol commonly used among IoT devices.
The source depends on the [Moquette broker] [2] and the [Eclipse Paho MQTT Client library] [3].

The current version of the proxy utilizes Google Cloud Pub/Sub as the backend Pub/Sub service.
If you're planning on running multiple instances of the proxy for scalability,
you must avoid running multiple instances on the same host.

## Building from source

Clone the repository and go into the source. Execute `mvn clean package`. The jar files will be
located in the `target` directory.

## Running the server

After going into the `target` directory execute the following:
`java -cp mqtt-cloud-pubsub-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar
com.google.cloud.pubsub.proxy.moquette.Server`

## Known Issues

1. Wildcard subscriptions are not supported in this version.
2. Clients may receive certain messages that were published before they subscribed.
3. The proxy server only supports QoS 1. Each message will be delivered at least once.
It is possible to receive duplicate messages.
4. Advanced MQTT features, namely retainable messages and last will messages,
are not supported for the multiple server setup.

[1]:https://mqtt.org
[2]:https://github.com/andsel/moquette
[3]:https://eclipse.org/paho/clients/java/
