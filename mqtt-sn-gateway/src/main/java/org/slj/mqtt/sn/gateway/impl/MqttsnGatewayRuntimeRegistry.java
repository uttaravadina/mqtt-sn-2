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

import org.slj.mqtt.sn.gateway.impl.gateway.MqttsnGatewayAdvertiseService;
import org.slj.mqtt.sn.gateway.impl.gateway.MqttsnGatewayMessageHandler;
import org.slj.mqtt.sn.gateway.impl.gateway.MqttsnGatewaySessionService;
import org.slj.mqtt.sn.gateway.spi.broker.IMqttsnBrokerConnectionFactory;
import org.slj.mqtt.sn.gateway.spi.broker.IMqttsnBrokerService;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayAdvertiseService;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewaySessionRegistryService;
import org.slj.mqtt.sn.impl.*;
import org.slj.mqtt.sn.impl.ram.MqttsnInMemoryMessageQueue;
import org.slj.mqtt.sn.impl.ram.MqttsnInMemoryMessageStateService;
import org.slj.mqtt.sn.impl.ram.MqttsnInMemorySubscriptionRegistry;
import org.slj.mqtt.sn.impl.ram.MqttsnInMemoryTopicRegistry;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.NetworkAddressRegistry;
import org.slj.mqtt.sn.spi.MqttsnRuntimeException;

public class MqttsnGatewayRuntimeRegistry extends AbstractMqttsnRuntimeRegistry implements IMqttsnGatewayRuntimeRegistry {

    private IMqttsnGatewayAdvertiseService advertiseService;
    private IMqttsnBrokerService brokerService;
    private IMqttsnBrokerConnectionFactory connectionFactory;
    private IMqttsnGatewaySessionRegistryService sessionService;

    public MqttsnGatewayRuntimeRegistry(MqttsnOptions options){
        super(options);
    }

    public static MqttsnGatewayRuntimeRegistry defaultConfiguration(MqttsnOptions options){
        MqttsnGatewayRuntimeRegistry registry = (MqttsnGatewayRuntimeRegistry) new MqttsnGatewayRuntimeRegistry(options).
                withGatewaySessionService(new MqttsnGatewaySessionService()).
                withGatewayAdvertiseService(new MqttsnGatewayAdvertiseService()).
                withMessageHandler(new MqttsnGatewayMessageHandler()).
                withNetworkAddressRegistry(new NetworkAddressRegistry(options.getMaxNetworkAddressEntries())).
                withMessageQueue(new MqttsnInMemoryMessageQueue()).
                withContextFactory(new MqttsnContextFactory()).
                withTopicRegistry(new MqttsnInMemoryTopicRegistry()).
                withQueueProcessor(new MqttsnMessageQueueProcessor(false)).
                withSubscriptionRegistry(new MqttsnInMemorySubscriptionRegistry()).
                withMessageStateService(new MqttsnInMemoryMessageStateService());
        return registry;
    }

    public MqttsnGatewayRuntimeRegistry withGatewayAdvertiseService(IMqttsnGatewayAdvertiseService advertiseService){
        this.advertiseService = advertiseService;
        return this;
    }

    public MqttsnGatewayRuntimeRegistry withBrokerConnectionFactory(IMqttsnBrokerConnectionFactory connectionFactory){
        this.connectionFactory = connectionFactory;
        return this;
    }

    public MqttsnGatewayRuntimeRegistry withGatewaySessionService(IMqttsnGatewaySessionRegistryService sessionService){
        this.sessionService = sessionService;
        return this;
    }

    public MqttsnGatewayRuntimeRegistry withBrokerService(IMqttsnBrokerService brokerService){
        this.brokerService = brokerService;
        return this;
    }

    @Override
    public IMqttsnGatewaySessionRegistryService getGatewaySessionService() {
        return sessionService;
    }

    @Override
    public IMqttsnBrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public IMqttsnBrokerConnectionFactory getBrokerConnectionFactory() {
        return connectionFactory;
    }

    public IMqttsnGatewayAdvertiseService getGatewayAdvertiseService() {
        return advertiseService;
    }

    @Override
    protected void validateOnStartup() throws MqttsnRuntimeException {
        if(brokerService == null) throw new MqttsnRuntimeException("message state service must be bound for valid runtime");
        if(connectionFactory == null) throw new MqttsnRuntimeException("connection factory must be bound for valid runtime");
        if(sessionService == null) throw new MqttsnRuntimeException("session service must be bound for valid runtime");
    }
}