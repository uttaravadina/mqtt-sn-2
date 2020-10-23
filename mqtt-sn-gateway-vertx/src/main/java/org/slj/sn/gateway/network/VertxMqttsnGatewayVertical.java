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

package org.slj.sn.gateway.network;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import org.slj.mqtt.sn.codec.MqttsnCodecs;
import org.slj.mqtt.sn.gateway.PahoMqttsnBrokerConnectionFactory;
import org.slj.mqtt.sn.gateway.impl.MqttsnGateway;
import org.slj.mqtt.sn.gateway.impl.MqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.gateway.impl.broker.MqttsnAggregatingBrokerService;
import org.slj.mqtt.sn.gateway.spi.broker.MqttsnBrokerOptions;
import org.slj.mqtt.sn.gateway.spi.gateway.MqttsnGatewayOptions;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;

import java.net.InetAddress;

public class VertxMqttsnGatewayVertical extends AbstractVerticle {

    protected MqttsnGateway gateway;

    @Override
    public void start() throws Exception {

        MqttsnUdpOptions udpOptions = new MqttsnUdpOptions().
                withHost(InetAddress.getLoopbackAddress().getHostAddress()).
                withPort(1029);

        MqttsnOptions gatewayOptions = new MqttsnGatewayOptions().
                withMaxConnectedClients(10);

        MqttsnBrokerOptions brokerOptions = new MqttsnBrokerOptions().
                withHost("YOUR-MQTT-HOST-HERE").
                withPort(1883).
                withUsername("YOUR-MQTT-USERNAME-HERE").
                withPassword("YOUR-MQTT-PASSWORD-HERE");

        AbstractMqttsnRuntimeRegistry registry = MqttsnGatewayRuntimeRegistry.defaultConfiguration(gatewayOptions).
                withBrokerConnectionFactory(new PahoMqttsnBrokerConnectionFactory()).
                withBrokerService(new MqttsnAggregatingBrokerService(brokerOptions)).
                withTransport(new MqttsnVertxUdpTransport(udpOptions, createSocket())).
                withCodec(MqttsnCodecs.MQTTSN_CODEC_VERSION_1_2);

        gateway = new MqttsnGateway();
        gateway.start(registry, gatewayOptions);
    }

    @Override
    public void stop() throws Exception {
        if(gateway != null){
            gateway.stop();
        }
    }

    protected DatagramSocket createSocket(){
        DatagramSocketOptions options = new DatagramSocketOptions();
        return getVertx().createDatagramSocket(options);
    }
}
