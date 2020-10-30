/*
 * Copyright (c) 2020 Simon Johnson <simon622 AT gmail DOT com>
 *
 * Find me on GitHub:
 * https://github.com/simon622
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.slj.mqtt.sn.client.impl;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.MqttsnCodecs;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;
import org.slj.mqtt.sn.net.MqttsnUdpTransport;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.utils.MqttsnUtils;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Example {
    public static void main(String[] args) throws Exception {

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
        }
    }
}
