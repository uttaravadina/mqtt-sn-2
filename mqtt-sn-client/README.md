# MQTT-SN Java Client
Full, dependency free java implementation of the MQTT-SN protocol specification for a client. 
Uses the mqtt-sn-codecs for wire transport and comes equipt with UDP network transport by default. 
NOTE: As with all the modules in this project, the persistence, transport and wire traffic layer is entirely pluggable.

## Quick start
Configure your details using the code below and run ExampleUsage.

```java
public class Example {
    public static void main(String[] args) throws Exception {

        //Very simple example which configures client runtime, connects for a 60 second session
        //binds a publish listener, subscribes to a topic, and publishes to the topic, and waits
        //for the publish to be received before disconnecting
       MqttsnUdpOptions udpOptions = new MqttsnClientUdpOptions();
       MqttsnOptions options = new MqttsnOptions().
               withNetworkAddressEntry("gatewayId",
                       NetworkAddress.localhost(MqttsnUdpOptions.DEFAULT_LOCAL_PORT)).
               withContextId("clientId1").
               withMaxWait(15000).
               withPredefinedTopic("my/example/topic/1", 1);

       AbstractMqttsnRuntimeRegistry registry = MqttsnClientRuntimeRegistry.defaultConfiguration(options).
               withTransport(new MqttsnUdpTransport(udpOptions)).
               withCodec(MqttsnCodecs.MQTTSN_CODEC_VERSION_1_2);

       AtomicInteger receiveCounter = new AtomicInteger();
       CountDownLatch latch = new CountDownLatch(1);
       try (MqttsnClient client = new MqttsnClient()) {
           client.start(registry);
           client.registerListener((String topic, int qos, byte[] data) -> {
               receiveCounter.incrementAndGet();
               System.err.println(String.format("received message [%s] [%s]",
                       receiveCounter.get(), new String(data, MqttsnConstants.CHARSET)));
               latch.countDown();
           });
           client.connect(360, true);
           client.subscribe("my/example/topic/1", 2);
           client.publish("my/example/topic/1", 1, MqttsnUtils.arrayOf(128, (byte) 0x01), true);
           latch.await(30, TimeUnit.SECONDS);
           client.disconnect();
       }
    }
}
```
