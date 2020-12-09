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

import org.slj.mqtt.sn.client.spi.IMqttsnClientRuntimeRegistry;
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.impl.AbstractMqttsnMessageHandler;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnAdvertise;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnRegister;

import java.util.logging.Level;

public class MqttsnClientMessageHandler
        extends AbstractMqttsnMessageHandler<IMqttsnClientRuntimeRegistry> {

    @Override
    protected IMqttsnMessage handleRegister(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException {
        MqttsnRegister register = (MqttsnRegister) message;
        if(register.getTopicId() > 0 && register.getTopicName() != null){
            registry.getTopicRegistry().register(context, register.getTopicName(), register.getTopicId());
        }
        return super.handleRegister(context, message);
    }

    @Override
    protected void handleAdvertise(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {

        MqttsnAdvertise advertise  = (MqttsnAdvertise) message;
        logger.log(Level.INFO, "client detected advertise message.. " + message);
//        registry.getNetworkRegistry().putContext();
    }

    @Override
    protected void beforeHandle(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    @Override
    protected void afterHandle(IMqttsnContext context, IMqttsnMessage message, IMqttsnMessage response) throws MqttsnException {
    }
}
