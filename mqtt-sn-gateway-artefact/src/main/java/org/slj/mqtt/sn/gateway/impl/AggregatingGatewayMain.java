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

package org.slj.mqtt.sn.gateway.impl;

import org.slj.mqtt.sn.codec.MqttsnCodecs;
import org.slj.mqtt.sn.gateway.impl.broker.MqttsnAggregatingBrokerService;
import org.slj.mqtt.sn.gateway.spi.broker.MqttsnBrokerOptions;
import org.slj.mqtt.sn.gateway.spi.gateway.MqttsnGatewayOptions;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;
import org.slj.mqtt.sn.net.MqttsnUdpTransport;

public class AggregatingGatewayMain {

    public static void main(String[] args) throws Exception {

        if(args.length < 6)
            throw new IllegalArgumentException("you must specify 6 arguments; <localPort>, <clientId>, <host>, <port>, <username> and <password>");

        int localPort = Integer.valueOf(args[0].trim());
        String clientId = args[1].trim();
        String host = args[2].trim();
        int port = Integer.valueOf(args[3].trim());
        String username = args[4].trim();
        String password = args[5].trim();

        MqttsnOptions gatewayOptions = new MqttsnGatewayOptions().
                withGatewayId(5).
                withMaxConnectedClients(10).
                withGatewayAdvertiseTime(10).
                withContextId(clientId).
                withPredefinedTopic("/my/example/topic/1", 1);

        MqttsnBrokerOptions brokerOptions = new MqttsnBrokerOptions().
                withHost(host).
                withPort(port).
                withUsername(username).
                withPassword(password);

        AbstractMqttsnRuntimeRegistry registry = MqttsnGatewayRuntimeRegistry.defaultConfiguration(gatewayOptions).
                withBrokerConnectionFactory(new PahoMqttsnBrokerConnectionFactory()).
                withBrokerService(new MqttsnAggregatingBrokerService(brokerOptions)).
                withTransport(new MqttsnUdpTransport(new MqttsnUdpOptions().withPort(localPort))).
                withCodec(MqttsnCodecs.MQTTSN_CODEC_VERSION_1_2);

        MqttsnGateway gateway = new MqttsnGateway();
        gateway.start(registry, gatewayOptions, true);
    }
}
