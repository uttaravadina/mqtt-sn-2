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
import org.slj.mqtt.sn.impl.AbstractMqttsnRuntime;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Provides a blocking command implementation, with the ability to handle transparent reconnection
 * during unsolicited disconnection events.
 *
 * Publishing occurs asynchronously and is managed by a FIFO queue. The size of the queue is determined
 * by the configuration supplied.
 *
 * Connect, subscribe, unsubscribe and disconnect ( &amp; sleep) are blocking calls which are considered successful on
 * receipt of the correlated acknowledgement message.
 *
 * Management of the sleeping client state can either be supervised by the application, or by the client itself. During
 * the sleep cycle, underlying resources (threads) are intelligently started and stopped. For example during sleep, the
 * queue processing is closed down, and restarted during the Connected state.
 *
 * The client is {@link java.io.Closeable}. On close, a remote DISCONNECT operation is run (if required) and all encapsulated
 * state (timers and threads) are stopped gracefully at best attempt. Once a client has been closed, it should be discarded and a new instance created
 * should a new connection be required.
 *
 * For example use, please refer to {@link Example}.
 */
public class MqttsnClient extends AbstractMqttsnRuntime implements IMqttsnClient {

    private volatile MqttsnSessionState state;
    private volatile int keepAlive;
    private volatile boolean cleanSession;
    private static int AUTOMATIC_PING_DIVISOR = 4;

    private Thread managedConnectionThread = null;

    public MqttsnClient(){
        this(false);
    }


    /**
     * Construct a new client instance specifying whether you want to client to automatically handle unsolicited DISCONNECT
     * events.
     *
     * @param managedConnection - You can choose to use managed connections which will actively monitor your connection with the remote gateway,
     *                          handling any unsolicited remote DISCONNECTs by RECONNECTING your session as well as ensuring the gateway keepAlive
     *                          is maintained (by issuing PINGs in periods of inactivity).
     */
    public MqttsnClient(boolean managedConnection){
        if(managedConnection){
            activateManagedConnection();
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
        callShutdown(registry.getTopicRegistry());
    }

    public IMqttsnSessionState getSessionState(){
        return state;
    }

    @Override
    /**
     * @see {@link IMqttsnClient#connect(int, boolean)}
     */
    public void connect(int keepAlive, boolean cleanSession) throws MqttsnException{
        if(!MqttsnUtils.validUInt16(keepAlive)){
            throw new MqttsnExpectationFailedException("invalid keepAlive supplied");
        }
        this.keepAlive = keepAlive;
        this.cleanSession = cleanSession;
        IMqttsnSessionState state = checkSession(false);
        synchronized (this) {
            if (state.getClientState() != MqttsnClientState.CONNECTED) {
                IMqttsnMessage message = registry.getMessageFactory().createConnect(
                        registry.getOptions().getContextId(), keepAlive, false, cleanSession);

                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                Optional<IMqttsnMessage> response =
                        registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.CONNECTED);
                startProcessing();
            }
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#publish(String, int, byte[])}
     */
    public void publish(String topicName, int QoS, byte[] data) throws MqttsnException{
        if(!MqttsnUtils.validQos(QoS)){
            throw new MqttsnExpectationFailedException("invalid QoS supplied");
        }
        if(!MqttsnUtils.validTopicName(topicName)){
            throw new MqttsnExpectationFailedException("invalid topicName supplied");
        }

        IMqttsnSessionState state = checkSession(QoS >= 0);
        UUID messageId = registry.getMessageRegistry().add(data, getMessageExpiry());
        if(!registry.getMessageQueue().offer(state.getContext(),
                new QueuedPublishMessage(
                        messageId, topicName, QoS))){
            throw new MqttsnExpectationFailedException("publish queue was full, publish operation not added");
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#subscribe(String, int)}
     */
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
    /**
     * @see {@link IMqttsnClient#unsubscribe(String)}
     */
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
    /**
     * @see {@link IMqttsnClient#supervisedSleepWithWake(int, int, int, boolean)}
     */
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
            long period = (int) Math.min(duration, timeLeft / 1000);
            //-- sleep for the wake after period
            try {
                long wake = Math.min(wakeAfterInterval, period);
                if(wake > 0){
                    logger.log(Level.INFO, String.format("waking after [%s] seconds", wake));
                    Thread.sleep(wake * 1000);
                    wake(maxWaitTime);
                } else {
                    break;
                }
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
                throw new MqttsnException(e);
            }
        }

        if(connectOnFinish){
            IMqttsnSessionState state = checkSession(false);
            connect(state.getKeepAlive(), false);
        } else {
            startProcessing();
            disconnect();
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#sleep(int)}
     */
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
            stopProcessing();
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#wake()}
     */
    public void wake()  throws MqttsnException{
        wake(registry.getOptions().getMaxWait());
    }

    @Override
    /**
     * @see {@link IMqttsnClient#wake(int)}
     */
    public void wake(int waitTime)  throws MqttsnException{
        IMqttsnSessionState state = checkSession(false);
        IMqttsnMessage message = registry.getMessageFactory().createPingreq(registry.getOptions().getContextId());
        synchronized (this){
            if(MqttsnUtils.in(state.getClientState(),
                    MqttsnClientState.ASLEEP)){
                startProcessing();
                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                state.setClientState(MqttsnClientState.AWAKE);
                Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token,
                        waitTime);
                stateChangeResponseCheck(state, token, response, MqttsnClientState.ASLEEP);
                stopProcessing();
            } else {
                throw new MqttsnExpectationFailedException("client cannot wake from a non-sleep state");
            }
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#ping()}
     */
    public void ping()  throws MqttsnException{
        IMqttsnSessionState state = checkSession(true);
        IMqttsnMessage message = registry.getMessageFactory().createPingreq(registry.getOptions().getContextId());
        synchronized (this){
            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
            registry.getMessageStateService().waitForCompletion(state.getContext(), token);
        }
    }

    @Override
    /**
     * @see {@link IMqttsnClient#disconnect()}
     */
    public void disconnect()  throws MqttsnException {
        disconnect(true);
    }

    private void disconnect(boolean sendDisconnect)  throws MqttsnException {
        try {
            IMqttsnSessionState state = checkSession(false);
            synchronized (this) {
                if(state != null){
                    clearState(state.getContext(),true);
                    try {
                        if (MqttsnUtils.in(state.getClientState(),
                                MqttsnClientState.CONNECTED, MqttsnClientState.ASLEEP, MqttsnClientState.AWAKE)) {
                            if(sendDisconnect){
                                IMqttsnMessage message = registry.getMessageFactory().createDisconnect();
                                MqttsnWaitToken token = registry.getMessageStateService().sendMessage(state.getContext(), message);
                                Optional<IMqttsnMessage> response = registry.getMessageStateService().waitForCompletion(state.getContext(), token);
                                stateChangeResponseCheck(state, token, response, MqttsnClientState.DISCONNECTED);
                            }
                        } else if (MqttsnUtils.in(state.getClientState(), MqttsnClientState.PENDING)) {
                            state.setClientState(MqttsnClientState.DISCONNECTED);
                        }
                    } finally {
                        state.setClientState(MqttsnClientState.DISCONNECTED);
                    }
                }
            }
        } finally {
            stopProcessing();
        }
    }

    private final void stopProcessing() throws MqttsnException {
        //-- ensure we stop message queue sending when we are not connected
        callShutdown(registry.getMessageHandler());
        callShutdown(registry.getMessageStateService());
    }

    private final void startProcessing() throws MqttsnException {
        callStartup(registry.getMessageStateService());
        callStartup(registry.getMessageHandler());
        registry.getMessageStateService().scheduleFlush(state.getContext());
    }

    private void clearState(IMqttsnContext context, boolean deepClear) throws MqttsnException {
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
    /**
     * @see {@link IMqttsnClient#close()}
     */
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

    private Date getMessageExpiry(){
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.HOUR, +1);
        return c.getTime();
    }

    /**
     * @see {@link IMqttsnClient#getClientId()}
     */
    public String getClientId(){
        return registry.getOptions().getContextId();
    }

    private void stateChangeResponseCheck(IMqttsnSessionState sessionState, MqttsnWaitToken token, Optional<IMqttsnMessage> response, MqttsnClientState newState)
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

    private MqttsnSessionState checkSession(boolean validateConnected) throws MqttsnException {
        MqttsnSessionState state = getOrDiscoverGatewaySession();
        if(validateConnected && state.getClientState() != MqttsnClientState.CONNECTED)
            throw new MqttsnRuntimeException("client not connected");
        return state;
    }

    private MqttsnSessionState getOrDiscoverGatewaySession() throws MqttsnException {
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

    private void activateManagedConnection(){
        if(managedConnectionThread == null){
            managedConnectionThread = new Thread(() -> {
                while(true){
                    try {
                        synchronized (managedConnectionThread){
                            long delta = Math.max(keepAlive, 60) / AUTOMATIC_PING_DIVISOR  * 1000;
                            logger.log(Level.INFO,
                                    String.format("managed connection monitor is running at time delta [%s], keepAlive [%s]...", delta, keepAlive));
                            managedConnectionThread.wait(delta);

                            if(running){
                                synchronized (this){ //-- we could receive a unsolicited disconnect during passive reconnection | ping..
                                    IMqttsnSessionState state = checkSession(false);
                                    if(state != null){
                                        if(state.getClientState() == MqttsnClientState.DISCONNECTED){
                                            logger.log(Level.INFO, "managed connection issuing new soft connection...");
                                            connect(keepAlive, false);
                                        }
                                        else if(state.getClientState() == MqttsnClientState.CONNECTED){
                                            if(keepAlive > 0){ //-- keepAlive 0 means alive forever, dont bother pinging
                                                Date lastMessageSent = registry.getMessageStateService().
                                                        getMessageLastSentToContext(state.getContext());
                                                if(lastMessageSent == null || System.currentTimeMillis() >
                                                        lastMessageSent.getTime() + delta ){
                                                    logger.log(Level.INFO, "managed connection issuing ping...");
                                                    ping();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch(Exception e){
                        logger.log(Level.SEVERE, "error on connection manager thread", e);
                    }
                }
            });
            managedConnectionThread.setPriority(Thread.MIN_PRIORITY);
            managedConnectionThread.setDaemon(true);
            managedConnectionThread.start();
        }
    }

    @Override
    public boolean handleRemoteDisconnect(IMqttsnContext context) {

        boolean shouldRecover = MqttsnUtils.in(state.getClientState(), MqttsnClientState.CONNECTED, MqttsnClientState.ASLEEP);
        try {
            logger.log(Level.WARNING, String.format("unsolicited disconnect received from gateway [%s]", context));
            disconnect(false);
        } catch(Exception e){
            logger.log(Level.WARNING, String.format("error handling unsolicited disconnect from gateway [%s]", context), e);
        }

        if(managedConnectionThread != null && shouldRecover){
            try {
                synchronized (managedConnectionThread){
                    managedConnectionThread.notify();
                }
            } catch(Exception e){
                logger.log(Level.WARNING, String.format("error encountered when trying to recover from unsolicited disconnect [%s]", context, e));
            }
        }

        //-- if its a server generated DISCONNECT unsolicited, dont reply
        return false;
    }

    @Override
    public boolean handleLocalDisconnectError(IMqttsnContext context, Throwable t) {
        return true;
    }
}