# MQTT Proxy for Cloud Pub/Sub

This project aims to provide a solution for utilizing Cloud Pub/Sub with IoT client devices through an MQTT interface.

MQTT is a lightweight protocol and hence suitable for IoT devices.
The source depends on the [Moquette broker] [1] and the [Eclipse Paho MQTT Client library] [2].

The current version of the proxy utilizes Google Cloud Pub/Sub as the backend Pub/Sub service.
If you're planning on running multiple instances of the proxy for scalability,
you must avoud running multipe instances on the same server.

[1]:https://github.com/andsel/moquette
[2]:https://eclipse.org/paho/clients/java/
