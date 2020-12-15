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
import org.slj.mqtt.sn.net.NetworkContext;
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
    protected INetworkAddressRegistry networkAddressRegistry;
    protected IMqttsnTopicRegistry topicRegistry;
    protected IMqttsnSubscriptionRegistry subscriptionRegistry;
    protected IMqttsnMessageStateService messageStateService;
    protected IMqttsnContextFactory contextFactory;
    protected IMqttsnMessageQueueProcessor queueProcessor;
    protected IMqttsnQueueProcessorStateService queueProcessorStateCheckService;
    protected IMqttsnMessageRegistry messageRegistry;
    protected IMqttsnPermissionService permissionService;
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
        //-- ensure initial definitions exist in the network registry
        if(options.getNetworkAddressEntries() != null && !options.getNetworkAddressEntries().isEmpty()){
            try {
                Iterator<String> itr = options.getNetworkAddressEntries().keySet().iterator();
                while(itr.hasNext()){
                    String key = itr.next();
                    NetworkAddress address = options.getNetworkAddressEntries().get(key);
                    NetworkContext networkContext = new NetworkContext(address);
                    networkContext.setMqttsnContext(new MqttsnContext(networkContext, key));
                    networkAddressRegistry.putContext(networkContext);
                }
            } catch(NetworkRegistryException e){
               throw new RuntimeException(e);
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
    public INetworkAddressRegistry getNetworkRegistry() {
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
    public IMqttsnPermissionService getPermissionService() {
        return permissionService;
    }

    public IMqttsnQueueProcessorStateService getQueueProcessorStateCheckService() {
        return queueProcessorStateCheckService;
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
    public IMqttsnMessageRegistry getMessageRegistry(){
        return messageRegistry;
    }

    @Override
    public List<IMqttsnTrafficListener> getTrafficListeners() {
        return trafficListeners;
    }

    @Override
    public IMqttsnContextFactory getContextFactory() {
        return contextFactory;
    }

    @Override
    public IMqttsnMessageQueueProcessor getQueueProcessor() {
        return queueProcessor;
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

    public AbstractMqttsnRuntimeRegistry withQueueProcessorStateCheck(IMqttsnQueueProcessorStateService queueProcessorStateCheckService){
        this.queueProcessorStateCheckService = queueProcessorStateCheckService;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withQueueProcessor(IMqttsnMessageQueueProcessor queueProcessor){
        this.queueProcessor = queueProcessor;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withContextFactory(IMqttsnContextFactory contextFactory){
        this.contextFactory = contextFactory;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withMessageRegistry(IMqttsnMessageRegistry messageRegistry){
        this.messageRegistry = messageRegistry;
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

    public AbstractMqttsnRuntimeRegistry withNetworkAddressRegistry(INetworkAddressRegistry networkAddressRegistry){
        this.networkAddressRegistry = networkAddressRegistry;
        return this;
    }

    public AbstractMqttsnRuntimeRegistry withPermissionService(IMqttsnPermissionService permissionService){
        this.permissionService = permissionService;
        return this;
    }

    protected void validateOnStartup() throws MqttsnRuntimeException {
        if(networkAddressRegistry == null) throw new MqttsnRuntimeException("network-registry must be bound for valid runtime");
        if(messageStateService == null) throw new MqttsnRuntimeException("message state service must be bound for valid runtime");
        if(transport == null) throw new MqttsnRuntimeException("transport must be bound for valid runtime");
        if(topicRegistry == null) throw new MqttsnRuntimeException("topic registry must be bound for valid runtime");
        if(codec == null) throw new MqttsnRuntimeException("codec must be bound for valid runtime");
        if(messageHandler == null) throw new MqttsnRuntimeException("message handler must be bound for valid runtime");
        if(messageQueue == null) throw new MqttsnRuntimeException("message queue must be bound for valid runtime");
        if(contextFactory == null) throw new MqttsnRuntimeException("context factory must be bound for valid runtime");
        if(queueProcessor == null) throw new MqttsnRuntimeException("queue processor must be bound for valid runtime");
        if(messageRegistry == null) throw new MqttsnRuntimeException("message registry must be bound for valid runtime");
    }
}
