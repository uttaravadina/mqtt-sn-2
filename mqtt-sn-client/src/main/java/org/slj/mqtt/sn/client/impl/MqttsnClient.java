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
import org.slj.mqtt.sn.client.spi.IMqttsnClient;
import org.slj.mqtt.sn.client.spi.IMqttsnClientRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntime;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MqttsnClient extends AbstractMqttsnRuntime implements IMqttsnClient {

    private volatile MqttsnSessionState state;
    private final boolean automaticReconnect;

    private volatile int keepAlive;
    private volatile boolean cleanSession;

    private Thread recoveryThread = null;


    public MqttsnClient(){
        this(false);
    }

    public MqttsnClient(boolean automaticReconnect){
        this.automaticReconnect = automaticReconnect;
        if(automaticReconnect){
            recoveryThread = new Thread(() -> {
                while(true){
                    try {
                        synchronized (recoveryThread){
                            recoveryThread.wait();
                            logger.log(Level.INFO, String.format("attempting to recover from unsolicited disconnect"));
                            connect(keepAlive, cleanSession);
                        }
                    } catch(Exception e){
                        logger.log(Level.SEVERE, "error on automatic recovery thread", e);
                    }
                }
            });
            recoveryThread.start();
        }
    }

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
        if(!MqttsnUtils.validUInt16(keepAlive)){
            throw new MqttsnExpectationFailedException("invalid keepAlive supplied");
        }
        keepAlive = keepAlive;
        cleanSession = cleanSession;
        IMqttsnSessionState state = checkSession(false);
        synchronized (this) {
            if (state.getClientState() != MqttsnClientState.CONNECTED) {
                IMqttsnMessage message = registry.getMessageFactory().createConnect(
                        registry.getOptions().getContextId(), keepAlive, false, cleanSession);

                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                Optional<IMqttsnMessage> response =
                        registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.CONNECTED);
                startQueueProcessing();
            }
        }
    }

    @Override
    public void publish(String topicName, int QoS, byte[] data) throws MqttsnException{
        if(!MqttsnUtils.validQos(QoS)){
            throw new MqttsnExpectationFailedException("invalid QoS supplied");
        }
        if(!MqttsnUtils.validTopicName(topicName)){
            throw new MqttsnExpectationFailedException("invalid topicName supplied");
        }
        IMqttsnSessionState state = checkSession(QoS >= 0);
        UUID messageId = registry.getMessageRegistry().add(data, true);
        if(!registry.getMessageQueue().offer(state.getContext(),
                new QueuedPublishMessage(
                        messageId, topicName, QoS))){
            throw new MqttsnExpectationFailedException("publish queue was full, publish operation not added");
        }
    }

    @Override
    public void subscribe(String topicName, int QoS) throws MqttsnException{
        if(!MqttsnUtils.validTopicName(topicName)){
            throw new MqttsnExpectationFailedException("invalid topicName supplied");
        }
        if(!MqttsnUtils.validQos(QoS)){
            throw new MqttsnExpectationFailedException("invalid QoS supplied");
        }
        IMqttsnSessionState state = checkSession(true);

        TopicInfo info = registry.getTopicRegistry().lookup(state.getContext(), topicName);
        IMqttsnMessage message = null;
        if(info == null){
            message = registry.getMessageFactory().createSubscribe(QoS, topicName);
        } else {
            message = registry.getMessageFactory().createSubscribe(QoS, info.getType(), info.getTopicId());
        }

        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            MqttsnUtils.responseCheck(token, response);
        }
    }

    @Override
    public void unsubscribe(String topicName) throws MqttsnException{
        if(!MqttsnUtils.validTopicName(topicName)){
            throw new MqttsnExpectationFailedException("invalid topicName supplied");
        }
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createUnsubscribe(topicName);
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            MqttsnUtils.responseCheck(token, response);
        }
    }

    @Override
    public void supervisedSleepWithWake(int duration, int wakeAfterInterval, int maxWaitTime, boolean connectOnFinish)  throws MqttsnException {

        if(!MqttsnUtils.validUInt16(duration)){
            throw new MqttsnExpectationFailedException("invalid duration supplied");
        }

        if(!MqttsnUtils.validUInt16(wakeAfterInterval)){
            throw new MqttsnExpectationFailedException("invalid wakeAfterInterval supplied");
        }

        if(wakeAfterInterval > duration)
           throw new MqttsnExpectationFailedException("sleep duration must be greater than the wake after period");

        long now = System.currentTimeMillis();
        long sleepUntil = now + (duration * 1000);
        sleep(duration);
        while(sleepUntil > (now = System.currentTimeMillis())){
            long timeLeft = sleepUntil - now;
            int period = (int) Math.min(duration, timeLeft / 1000);
            //-- sleep for the wake after period
            try {
                long wake = Math.min(wakeAfterInterval, period);
                logger.log(Level.INFO, String.format("waking after [%s] seconds", wake));
                Thread.sleep(wake * 1000);
                wake(maxWaitTime);
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
                throw new MqttsnException(e);
            }
        }

        if(connectOnFinish){
            IMqttsnSessionState state = checkSession(false);
            connect(state.getKeepAlive(), false);
        } else {
            disconnect();
        }
    }

    @Override
    public void sleep(int duration)  throws MqttsnException{
        if(!MqttsnUtils.validUInt16(duration)){
            throw new MqttsnExpectationFailedException("invalid duration supplied");
        }
        logger.log(Level.INFO, String.format("sleeping for [%s] seconds", duration));
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createDisconnect(duration);
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
            stateChangeResponseCheck(state, token, response, MqttsnClientState.ASLEEP);
            clearState(state.getContext(),false);
            stopQueueProcessing();
        }
    }

    @Override
    public void wake()  throws MqttsnException{
        wake(registry.getOptions().getMaxWait());
    }

    @Override
    public void wake(int waitTime)  throws MqttsnException{
        IMqttsnSessionState state = checkSession(false);
        IMqttsnMessage message = registry.getMessageFactory().createPingreq(registry.getOptions().getContextId());
        synchronized (this){
            if(MqttsnUtils.in(state.getClientState(),
                    MqttsnClientState.ASLEEP)){
                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                state.setClientState(MqttsnClientState.AWAKE);
                Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token,
                        waitTime);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.ASLEEP);
            } else {
                throw new MqttsnExpectationFailedException("client connect wake from a non-connected state");
            }
        }
    }

    @Override
    public void disconnect()  throws MqttsnException {
        disconnect(true);
    }

    protected void disconnect(boolean sendResponse)  throws MqttsnException {
        try {
            IMqttsnSessionState state = checkSession(false);
            synchronized (this) {
                if(state != null){
                    if (MqttsnUtils.in(state.getClientState(),
                            MqttsnClientState.CONNECTED, MqttsnClientState.ASLEEP, MqttsnClientState.AWAKE)) {
                        registry.getMessageQueue().clear(state.getContext());
                        if(sendResponse){
                            IMqttsnMessage message = registry.getMessageFactory().createDisconnect();
                            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                            Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                            stateChangeResponseCheck(state, token, response, MqttsnClientState.DISCONNECTED);
                        } else {
                            state.setClientState(MqttsnClientState.DISCONNECTED);
                        }
                    } else if (MqttsnUtils.in(state.getClientState(), MqttsnClientState.PENDING)) {
                        state.setClientState(MqttsnClientState.DISCONNECTED);
                    }
                    clearState(state.getContext(),true);
                }
            }
        } finally {
            stopQueueProcessing();
        }
    }

    protected void stopQueueProcessing() throws MqttsnException {
        //-- ensure we stop message queue sending when we are not connected
        IMqttsnService queueService = ((IMqttsnClientRuntimeRegistry)registry).getClientQueueService();
        synchronized (queueService){
            if(queueService.running()){
                callShutdown(queueService);
            }
        }
        IMqttsnService stateService = ((IMqttsnClientRuntimeRegistry)registry).getMessageStateService();
        synchronized (stateService){
            if(stateService.running()){
                callShutdown(stateService);
            }
        }
    }

    protected void startQueueProcessing() throws MqttsnException {
        IMqttsnService queueService = ((IMqttsnClientRuntimeRegistry)registry).getClientQueueService();
        synchronized (queueService){
            if(!queueService.running()){
                callStartup(queueService);
            }
        }
        IMqttsnService stateService = registry.getMessageStateService();
        synchronized (stateService){
            if(!stateService.running()){
                callStartup(stateService);
            }
        }
    }

    protected void clearState(IMqttsnContext context, boolean deepClear) throws MqttsnException {
        //-- unsolicited disconnect notify to the application
        registry.getMessageStateService().clearInflight(context);
        registry.getTopicRegistry().clear(context,
                registry.getOptions().isSleepClearsRegistrations());
        if(deepClear){
            registry.getSubscriptionRegistry().clear(context);
            registry.getMessageQueue().clear(context);
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
    public boolean disconnectReceived(IMqttsnContext context) {

        boolean shouldRecover = MqttsnUtils.in(state.getClientState(), MqttsnClientState.CONNECTED, MqttsnClientState.ASLEEP);
        try {
            logger.log(Level.WARNING, String.format("unsolicited disconnect received from gateway [%s]", context));
            disconnect(false);
        } catch(Exception e){
            logger.log(Level.WARNING, String.format("error handling unsolicited disconnect from gateway [%s]", context), e);
        }

        if(automaticReconnect && shouldRecover){
            try {
                synchronized (recoveryThread){
                    recoveryThread.notify();
                }
            } catch(Exception e){
                logger.log(Level.WARNING, String.format("error encountered when trying to recover from unsolicited disconnect [%s]", context, e));
            }
        }

        //-- if its a server generated DISCONNECT unsolicited, dont reply
        return false;
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

    protected MqttsnSessionState checkSession(boolean validateConnected) throws MqttsnException {
        MqttsnSessionState state = getOrDiscoverGatewaySession();
        if(validateConnected && state.getClientState() != MqttsnClientState.CONNECTED)
            throw new MqttsnRuntimeException("client not connected");
        return state;
    }

    protected MqttsnSessionState getOrDiscoverGatewaySession() throws MqttsnException {
        if(state == null){
            synchronized (this){
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
            }
        }

        return state;
    }
}