package org.slj.mqtt.sn.client.impl;

import org.slj.mqtt.sn.codec.MqttsnCodecs;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;
import org.slj.mqtt.sn.net.MqttsnUdpTransport;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.IMqttsnTrafficListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TSMain {

    public static void main(String[] args) throws Exception {

        int NUM = 1;
        MqttsnUdpOptions udpOptions = new MqttsnUdpOptions();
        MqttsnOptions options = new MqttsnOptions().
                withNetworkAddressEntry("gatewayId",
                        NetworkAddress.from(2442, "54.212.123.174")).
                withContextId("foo").
                withMaxWait(15000).
                withPredefinedTopic("foobar", 100);

        AbstractMqttsnRuntimeRegistry registry = MqttsnClientRuntimeRegistry.defaultConfiguration(options).
                withTransport(new MqttsnUdpTransport(udpOptions)).
                withTrafficListener(new IMqttsnTrafficListener() {
                    @Override
                    public void trafficSent(INetworkContext arg0, byte[] arg1, IMqttsnMessage message) {
//					System.err.println("sent " + message);
                    }

                    @Override
                    public void trafficReceived(INetworkContext arg0, byte[] arg1, IMqttsnMessage message) {
//					System.err.println("received " + message);
                    }
                }).
                withCodec(MqttsnCodecs.MQTTSN_CODEC_VERSION_1_2);

        AtomicInteger receiveCounter = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(NUM);
        try (MqttsnClient client = new MqttsnClient()) {
            client.start(registry);
            client.registerReceivedListener((IMqttsnContext context, String topic, int qos, byte[] data) -> {
                receiveCounter.incrementAndGet();
                System.err.println(String.format("received publish message [%s] to [%s] -> [%s] bytes",
                        receiveCounter.get(), topic, data.length));
                latch.countDown();
            });

            client.connect(10, true);
            client.subscribe("my/example/topic/1", 2);
            client.subscribe("foobar", 2);
            client.supervisedSleepWithWake(60 * 10, 30, false);
        }
    }
}
