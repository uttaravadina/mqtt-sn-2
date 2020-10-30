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

package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.model.MqttsnContext;
import org.slj.mqtt.sn.model.MqttsnOptions;
import org.slj.mqtt.sn.model.NetworkContext;
import org.slj.mqtt.sn.net.NetworkAddressRegistry;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.spi.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractMqttsnRuntimeRegistry implements IMqttsnRuntimeRegistry {

    protected MqttsnOptions options;

    //-- obtained lazily from the codec --//
    protected IMqttsnMessageFactory factory;

    protected IMqttsnCodec codec;
    protected IMqttsnMessageHandler messageHandler;
    protected IMqttsnMessageQueue messageQueue;
    protected IMqttsnTransport transport;
    protected AbstractMqttsnRuntime runtime;
    protected NetworkAddressRegistry networkAddressRegistry;
    protected IMqttsnTopicRegistry topicRegistry;
    protected IMqttsnSubscriptionRegistry subscriptionRegistry;
    protected IMqttsnMessageStateService messageStateService;
    protected List<IMqttsnTrafficListener> trafficListeners;

    public AbstractMqttsnRuntimeRegistry(MqttsnOptions options){
        setOptions(options);
    }

    @Override
    public void init() {
        validateOnStartup();
        initNetworkRegistry();
        factory = codec.createMessageFactory();
    }

    protected void initNetworkRegistry(){
        if(networkAddressRegistry == null){
            networkAddressRegistry = new NetworkAddressRegistry(options.getMaxNetworkAddressEntries());
        }
        if(options.getNetworkAddressEntries() != null && !options.getNetworkAddressEntries().isEmpty()){
            Iterator<String> itr = options.getNetworkAddressEntries().keySet().iterator();
            while(itr.hasNext()){
                String key = itr.next();
                NetworkAddress address = options.getNetworkAddressEntries().get(key);
                networkAddressRegistry.putContext(new NetworkContext(address, new MqttsnContext(key)));
            }
        }
    }

    @Override
    public void setOptions(MqttsnOptions options) {
        this.options = options;
    }

    @Override
    public MqttsnOptions getOptions() {
        return options;
    }

    @Override
    public NetworkAddressRegistry getNetworkRegistry() {
        return networkAddressRegistry;
    }

    @Override
    public void setRuntime(AbstractMqttsnRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public AbstractMqttsnRuntime getRuntime() {
        return runtime;
    }

    @Override
    public IMqttsnCodec getCodec() {
        return codec;
    }

    @Override
    public IMqttsnMessageHandler getMessageHandler() {
        return messageHandler;
    }

    @Override
    public IMqttsnTransport getTransport() {
        return transport;
    }

    @Override
    public IMqttsnMessageFactory getMessageFactory(){
        return factory;
    }

    @Override
    public IMqttsnMessageQueue getMessageQueue() {
        return messageQueue;
    }

    public NetworkAddressRegistry getNetworkAddressRegistry() {
        return networkAddressRegistry;
    }

    @Override
    public IMqttsnTopicRegistry getTopicRegistry() {
        return topicRegistry;
    }

    @Override
    public IMqttsnSubscriptionRegistry getSubscriptionRegistry() {
        return subscriptionRegistry;
    }

    @Override
    public IMqttsnMessageStateService getMessageStateService() {
        return messageStateService;
    }

    @Override
    public List<IMqttsnTrafficListener> getTrafficListeners() {
        return trafficListeners;
    }

    public AbstractMqttsnRuntimeRegistry withTrafficListener(IMqttsnTrafficListener trafficListener){
        if(trafficListeners == null){
            synchronized (this){
                if(trafficListeners == null){
                    trafficListeners = new ArrayList<>();
                }
            }
        }
        trafficListeners.add(trafficListener);
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withMessageStateService(IMqttsnMessageStateService messageStateService){
        this.messageStateService = messageStateService;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withSubscriptionRegistry(IMqttsnSubscriptionRegistry subscriptionRegistry){
        this.subscriptionRegistry = subscriptionRegistry;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withTopicRegistry(IMqttsnTopicRegistry topicRegistry){
        this.topicRegistry = topicRegistry;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withMessageQueue(IMqttsnMessageQueue messageQueue){
        this.messageQueue = messageQueue;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withCodec(IMqttsnCodec codec){
        this.codec = codec;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withMessageHandler(IMqttsnMessageHandler handler){
        this.messageHandler = handler;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withTransport(IMqttsnTransport transport){
        this.transport = transport;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withNetworkAddressRegistry(NetworkAddressRegistry networkAddressRegistry){
        this.networkAddressRegistry = networkAddressRegistry;
        return this;
    }

    protected void validateOnStartup() throws MqttsnRuntimeException {
        if(messageStateService == null) throw new MqttsnRuntimeException("message state service must be bound for valid runtime");
        if(transport == null) throw new MqttsnRuntimeException("transport must be bound for valid runtime");
        if(topicRegistry == null) throw new MqttsnRuntimeException("topic registry must be bound for valid runtime");
        if(codec == null) throw new MqttsnRuntimeException("codec must be bound for valid runtime");
        if(messageHandler == null) throw new MqttsnRuntimeException("message handler must be bound for valid runtime");
        if(messageQueue == null) throw new MqttsnRuntimeException("message queue must be bound for valid runtime");
    }
}
