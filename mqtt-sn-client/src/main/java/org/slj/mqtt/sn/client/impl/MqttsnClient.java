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

import org.slj.mqtt.sn.client.spi.IMqttsnClient;
import org.slj.mqtt.sn.client.spi.IMqttsnClientRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntime;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MqttsnClient extends AbstractMqttsnRuntime implements IMqttsnClient {

    private volatile MqttsnSessionState state;

    @Override
    protected void startupServices(IMqttsnRuntimeRegistry registry) throws MqttsnException {

        try {
            Optional<INetworkContext> optionalContext = registry.getNetworkRegistry().first();
            if(!registry.getOptions().isEnableDiscovery() &&
                    !optionalContext.isPresent()){
                throw new MqttsnRuntimeException("unable to launch non-discoverable client without configured gateway");
            }
        } catch(NetworkRegistryException e){
            throw new MqttsnException("error using network registry", e);
        }

        callStartup(registry.getMessageStateService());
        callStartup(registry.getMessageHandler());
        callStartup(registry.getMessageQueue());
        callStartup(registry.getMessageRegistry());
        callStartup(registry.getContextFactory());
        callStartup(registry.getSubscriptionRegistry());
        callStartup(registry.getTopicRegistry());
        callStartup(((IMqttsnClientRuntimeRegistry)registry).getClientQueueService());
        callStartup(registry.getQueueProcessor());
        callStartup(registry.getTransport());

    }

    @Override
    protected void stopServices(IMqttsnRuntimeRegistry registry) throws MqttsnException {
        callShutdown(registry.getTransport());
        callShutdown(registry.getQueueProcessor());
        callShutdown(registry.getMessageStateService());
        callShutdown(registry.getMessageHandler());
        callShutdown(registry.getMessageQueue());
        callShutdown(registry.getMessageRegistry());
        callShutdown(registry.getContextFactory());
        callShutdown(registry.getSubscriptionRegistry());
        callShutdown(((IMqttsnClientRuntimeRegistry)registry).getClientQueueService());
        callShutdown(registry.getTopicRegistry());
    }

    public IMqttsnSessionState getSessionState(){
        return state;
    }

    @Override
    public void connect(int keepAlive, boolean cleanSession) throws MqttsnException{
        IMqttsnSessionState state = checkSession(false);
        synchronized (this) {
            if (state.getClientState() != MqttsnClientState.CONNECTED) {
                IMqttsnMessage message = registry.getMessageFactory().createConnect(
                        registry.getOptions().getContextId(), keepAlive, false, cleanSession);

                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                Optional<IMqttsnMessage> response =
                        registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.CONNECTED);
                ensureAwake();
            }
        }
    }

    @Override
    public void publish(String topicName, int QoS, byte[] data) throws MqttsnException{
        IMqttsnSessionState state = checkSession(QoS >= 0);
        registry.getMessageQueue().offer(state.getContext(),
                new QueuedPublishMessage(
                        registry.getMessageRegistry().add(data, true), topicName, QoS));
    }

    @Override
    public void subscribe(String topicName, int QoS) throws MqttsnException{
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createSubscribe(QoS, topicName);
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            MqttsnUtils.responseCheck(token, response);
        }
    }

    @Override
    public void unsubscribe(String topicName) throws MqttsnException{
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createUnsubscribe(topicName);
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            MqttsnUtils.responseCheck(token, response);
        }
    }

    @Override
    public void sleep(int keepAlive)  throws MqttsnException{
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createDisconnect(keepAlive);
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            stateChangeResponseCheck(state, token, response, MqttsnClientState.ASLEEP);
            pauseRuntime();
        }
    }

    @Override
    public void wake()  throws MqttsnException{
        IMqttsnSessionState state = checkSession(false);
        IMqttsnMessage message = registry.getMessageFactory().createPingreq(null);
        synchronized (this){
            if(MqttsnUtils.in(state.getClientState(),
                    MqttsnClientState.ASLEEP)){
                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                state.setClientState(MqttsnClientState.AWAKE);
                ensureAwake();
                Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.ASLEEP);
            } else {
                throw new MqttsnExpectationFailedException("client connect wake from a non-connected state");
            }
        }
    }

    @Override
    public void disconnect()  throws MqttsnException {
        try {
            IMqttsnSessionState state = checkSession(false);
            synchronized (this) {
                if (MqttsnUtils.in(state.getClientState(),
                        MqttsnClientState.CONNECTED, MqttsnClientState.ASLEEP, MqttsnClientState.AWAKE)) {
                    IMqttsnMessage message = registry.getMessageFactory().createDisconnect();
                    registry.getMessageQueue().clear(state.getContext());
                    MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                    Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                    stateChangeResponseCheck(state, token, response, MqttsnClientState.DISCONNECTED);

                } else if (MqttsnUtils.in(state.getClientState(), MqttsnClientState.PENDING)) {
                    state.setClientState(MqttsnClientState.DISCONNECTED);
                }
            }
        } finally {
            pauseRuntime();
        }
    }

    protected void pauseRuntime() throws MqttsnException {
        //-- ensure we stop message queue sending when we are not connected
        IMqttsnService service = ((IMqttsnClientRuntimeRegistry)registry).getClientQueueService();
        synchronized (service){
            if(service.running()){
                callShutdown(service);
            }
        }
    }

    protected void ensureAwake() throws MqttsnException {
        IMqttsnService service = ((IMqttsnClientRuntimeRegistry)registry).getClientQueueService();
        synchronized (service){
            if(!service.running()){
                callStartup(service);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            disconnect();
        } catch(MqttsnException e){
            throw new IOException(e);
        } finally {
            try {
                if(registry != null)
                    stop();
            }
            catch(MqttsnException e){
                throw new IOException (e);
            }
        }
    }

    public String getClientId(){
        return registry.getOptions().getContextId();
    }

    @Override
    public void disconnectReceived(IMqttsnContext context) {
        try {
            logger.log(Level.SEVERE, String.format("unsolicited disconnect received from gateway [%s]", context));
            if(state != null){
                if(state.getContext().equals(context)){
                    state.setClientState(MqttsnClientState.DISCONNECTED);
                }
            }
        } catch(Exception e){
            logger.log(Level.WARNING, String.format("error handling unsolicited disconnect from gateway [%s]", context), e);
        }
    }

    protected void stateChangeResponseCheck(IMqttsnSessionState sessionState, MqttsnWaitToken token, Optional<IMqttsnMessage> response, MqttsnClientState newState)
            throws MqttsnExpectationFailedException {
        try {
            MqttsnUtils.responseCheck(token, response);
            if(response.isPresent() &&
                    !response.get().isErrorMessage()){
                sessionState.setClientState(newState);
            }
        } catch(MqttsnExpectationFailedException e){
            logger.log(Level.SEVERE, "operation could not be completed, error in response");
            throw e;
        }
    }

    protected MqttsnSessionState checkSession(boolean validate) throws MqttsnException {
        MqttsnSessionState state = getOrDiscoverGatewaySession();
        if(validate && state.getClientState() != MqttsnClientState.CONNECTED)
            throw new MqttsnRuntimeException("client not connected");
        return state;
    }

    protected MqttsnSessionState getOrDiscoverGatewaySession() throws MqttsnException {
        if(state == null){
            try {
                logger.log(Level.INFO, "discover gateway...");
                Optional<INetworkContext> optionalMqttsnContext =
                        registry.getNetworkRegistry().waitForContext(60, TimeUnit.MINUTES);
                if(optionalMqttsnContext.isPresent()){
                    INetworkContext gatewayContext = optionalMqttsnContext.get();
                    state = new MqttsnSessionState(gatewayContext.getMqttsnContext(), MqttsnClientState.PENDING);
                    logger.log(Level.INFO, String.format("discovery located a gateway for use [%s]", gatewayContext));
                } else {
                    throw new MqttsnException("unable to discovery gateway within specified timeout");
                }
            } catch(NetworkRegistryException | InterruptedException e){
                throw new MqttsnException("discovery was interrupted and no gateway was found", e);
            }
        }
        return state;
    }
}

