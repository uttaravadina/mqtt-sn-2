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

import org.slj.mqtt.sn.gateway.spi.PublishResult;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntime;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.IMqttsnPublishReceivedListener;
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.util.logging.Level;

public class MqttsnGateway extends AbstractMqttsnRuntime {

    protected void startupServices(IMqttsnRuntimeRegistry runtime) throws MqttsnException {

        //-- ensure we start all the startable services
        callStartup(runtime.getMessageHandler());
        callStartup(runtime.getMessageQueue());
        callStartup(runtime.getMessageRegistry());
        callStartup(runtime.getTopicRegistry());
        callStartup(runtime.getSubscriptionRegistry());
        callStartup(runtime.getMessageStateService());
        callStartup(runtime.getQueueProcessor());
        callStartup(runtime.getContextFactory());

        //-- start the network last
        callStartup(((IMqttsnGatewayRuntimeRegistry)runtime).getGatewaySessionService());
        callStartup(((IMqttsnGatewayRuntimeRegistry)runtime).getBrokerConnectionFactory());
        callStartup(((IMqttsnGatewayRuntimeRegistry)runtime).getBrokerService());

        //-- start discovery
        if(runtime.getOptions().isEnableDiscovery()){
            callStartup(((IMqttsnGatewayRuntimeRegistry)runtime).getGatewayAdvertiseService());
        }

        callStartup(runtime.getTransport());

        //-- notify the broker of confirmed messahe
        registerReceivedListener(new IMqttsnPublishReceivedListener() {
            @Override
            public void receive(IMqttsnContext context, String topicName, int QoS, byte[] data) {
                try {
                    ((IMqttsnGatewayRuntimeRegistry)registry).getBrokerService().publish(context, topicName, QoS, data);
                } catch(MqttsnException e){
                    logger.log(Level.SEVERE, "error publishing message to broker");
                }
            }
        });
    }

    /*
    @Override
    public PublishResult publish(IMqttsnSessionState state, TopicInfo topicInfo, int QoS, byte[] data) throws MqttsnException {
        try {
            IMqttsnContext context = state == null ? null : state.getContext();
            String topicPath = registry.getTopicRegistry().topicPath(context, topicInfo, context != null);
            return registry.getBrokerService().publish(context, topicPath, QoS, data);
        } catch(MqttsnBrokerException e){
            throw new MqttsnException(e);
        }
    }
     */

    public void stopServices(IMqttsnRuntimeRegistry runtime) throws MqttsnException {

        //-- stop the networks first
        callShutdown(runtime.getTransport());

        if(runtime.getOptions().isEnableDiscovery()){
            callShutdown(((IMqttsnGatewayRuntimeRegistry)runtime).getGatewayAdvertiseService());
        }

        callShutdown(((IMqttsnGatewayRuntimeRegistry)runtime).getGatewaySessionService());
        callShutdown(((IMqttsnGatewayRuntimeRegistry)runtime).getBrokerConnectionFactory());
        callShutdown(((IMqttsnGatewayRuntimeRegistry)runtime).getBrokerService());

        //-- ensure we start all the startable services

        callShutdown(runtime.getContextFactory());
        callShutdown(runtime.getMessageHandler());
        callShutdown(runtime.getMessageQueue());
        callShutdown(runtime.getMessageRegistry());
        callShutdown(runtime.getTopicRegistry());
        callShutdown(runtime.getSubscriptionRegistry());
        callShutdown(runtime.getQueueProcessor());
        callShutdown(runtime.getMessageStateService());
    }


}