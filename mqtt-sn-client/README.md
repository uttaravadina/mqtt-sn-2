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

### MQTT-SN client configuration options
The default client behaviour can be customised using configuration options. Sensible defaults have been specified which allow it to all work out of the box.
Many of the options below are applicable for both the client and gateway runtimes. 

Options | Default Value | Type | Description
------------ | ------------- | ------------- | -------------
contextId | NULL | String | This is used as either the clientId (when in a client runtime) or the gatewayId (when in a gateway runtime). **NB: This is a required field and must be set by the application.**
maxWait | 10000 | int | Time in milliseconds to wait for a confirmation message where required. When calling a blocking method, this is the time the method will block until either the confirmation is received OR the timeout expires.
threadHandoffFromTransport | true | boolean | Should the transport layer delegate to and from the handler layer using a thread hand-off. **NB: Depends on your transport implementation as to whether you should block.** 
blockSendOnPreviousConfirmation | true | boolean | The client can choose to NOT waitOnCompletion of a previous delivery. If the application then immediately send a new message, the client will either fail-fast with a runtime exception or optionally block until the previous operation completes.
enableDiscovery | true | boolean | When discovery is enabled the client will listen for broadcast messages from local gateways and add them to its network registry as it finds them.
maxTopicsInRegistry | 128 | int | Max number of topics which can reside in the CLIENT registry. This does NOT include predefined alias's.
msgIdStartAt | 1 | int (max. 65535) | Starting number for message Ids sent from the client to the gateways (each gateway has a unique count).
aliasStartAt | 1 | int (max. 65535) | Starting number for alias's used to store topic values (NB: only applicable to gateways).
maxMessagesInflight | 1 | int (max. 65535) | In theory, a gateway and broker can have multiple messages inflight concurrently. The spec suggests only 1 confirmation message is inflight at any given time. (NB: do NOT change this).
maxMessagesInQueue | 100 | int | Max number of messages allowed in a client's queue. When the max is reached any new messages will be discarded.
requeueOnInflightTimeout | true | boolean | When a publish message fails to confirm, should it be requeued for DUP sending at a later point.
predefinedTopics | Config| Map | Where a client or gateway both know a topic alias in advance, any messages or subscriptions to the topic will be made using the predefined IDs. 
networkAddressEntries | Config | Map | You can prespecify known locations for gateways and clients in the network address registry. NB. The runtime will dynamically update the registry with new clients / gateways as they are discovered. In the case of clients, they are unable to connect or message until at least 1 gateway is defined in config OR discovered.
