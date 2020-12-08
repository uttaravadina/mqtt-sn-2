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

import org.slj.mqtt.sn.client.spi.IMqttsnClientQueueService;
import org.slj.mqtt.sn.client.spi.IMqttsnClientRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.impl.MqttsnContextFactory;
import org.slj.mqtt.sn.impl.MqttsnMessageQueueProcessor;
import org.slj.mqtt.sn.impl.ram.*;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.net.NetworkAddressRegistry;
import org.slj.mqtt.sn.spi.MqttsnRuntimeException;

public class MqttsnClientRuntimeRegistry extends AbstractMqttsnRuntimeRegistry implements IMqttsnClientRuntimeRegistry {

    protected IMqttsnClientQueueService queueService;

    public MqttsnClientRuntimeRegistry(MqttsnOptions options){
        super(options);
    }

    public static MqttsnClientRuntimeRegistry defaultConfiguration(MqttsnOptions options){
        MqttsnClientRuntimeRegistry registry = (MqttsnClientRuntimeRegistry) new MqttsnClientRuntimeRegistry(options).
                withClientQueueService(new MqttsnClientQueueService()).
                withContextFactory(new MqttsnContextFactory()).
                withMessageHandler(new MqttsnClientMessageHandler()).
                withMessageRegistry(new MqttsnInMemoryMessageRegistry()).
                withNetworkAddressRegistry(new NetworkAddressRegistry(options.getMaxNetworkAddressEntries())).
                withTopicRegistry(new MqttsnInMemoryTopicRegistry()).
                withSubscriptionRegistry(new MqttsnInMemorySubscriptionRegistry()).
                withMessageQueue(new MqttsnInMemoryMessageQueue()).
                withQueueProcessor(new MqttsnMessageQueueProcessor(true)).
                withMessageStateService(new MqttsnInMemoryMessageStateService());
        return registry;
    }

    public MqttsnClientRuntimeRegistry withClientQueueService(IMqttsnClientQueueService queueService){
        this.queueService = queueService;
        return this;
    }

    public IMqttsnClientQueueService getClientQueueService() {
        return queueService;
    }

    @Override
    protected void validateOnStartup() throws MqttsnRuntimeException {
        super.validateOnStartup();
        if(queueService == null) throw new MqttsnRuntimeException("message queue service must be bound for valid runtime");
    }
}
