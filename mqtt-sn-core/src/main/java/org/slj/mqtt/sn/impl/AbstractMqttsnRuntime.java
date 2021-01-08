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

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.model.MqttsnOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractMqttsnRuntime {

    protected Logger logger = Logger.getLogger(getClass().getName());
    protected IMqttsnRuntimeRegistry registry;

    protected List<IMqttsnPublishReceivedListener> receivedListeners
            = Collections.synchronizedList(new ArrayList<>());
    protected List<IMqttsnPublishSentListener> sentListeners
            = Collections.synchronizedList(new ArrayList<>());

    protected ExecutorService executorService;
    protected CountDownLatch startupLatch;
    protected volatile boolean running = false;
    protected long startedAt;
    private final Object monitor = new Object();

    public final void start(IMqttsnRuntimeRegistry reg) throws MqttsnException {
        start(reg, false);
    }

    public final void start(IMqttsnRuntimeRegistry reg, boolean join) throws MqttsnException {
        if(!running){
            int threadCount = reg.getOptions().getHandoffThreadCount();
            executorService =
                    Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
                        int count = 0;
                        ThreadGroup tg = new ThreadGroup("mqtt-sn-worker");

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(tg, r, "mqtt-sn-worker-thread-" + ++count);
                            t.setPriority(Thread.MIN_PRIORITY + 1);
                            t.setDaemon(true);
                            return t;
                        }
                    });
            startedAt = System.currentTimeMillis();
            setupEnvironment();
            registry = reg;
            startupLatch = new CountDownLatch(1);
            running = true;
            registry.setRuntime(this);
            registry.init();
            bindShutdownHook();
            logger.log(Level.INFO, "starting mqttsn-environment..");
            startupServices(registry);
            startupLatch.countDown();
            logger.log(Level.INFO, String.format("mqttsn-environment started successfully in [%s]", System.currentTimeMillis() - startedAt));
            if(join){
                while(running){
                    synchronized (monitor){
                        try {
                            monitor.wait();
                        } catch(InterruptedException e){
                            Thread.currentThread().interrupt();
                            throw new MqttsnException(e);
                        }
                    }
                }
            }
        }
    }

    public final void stop() throws MqttsnException {
        if(running){
            logger.log(Level.INFO, "stopping mqttsn-environment..");
            stopServices(registry);
            running = false;
            receivedListeners.clear();
            sentListeners.clear();
            try {
                if(!executorService.isShutdown()){
                    executorService.shutdown();
                }
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
            } finally {
                if (!executorService.isTerminated()) {
                    executorService.shutdownNow();
                }
            }
            synchronized (monitor){
                monitor.notifyAll();
            }
        }
    }

    protected void bindShutdownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                AbstractMqttsnRuntime.this.stop();
            } catch(Exception e){
                logger.log(Level.SEVERE, "encountered error executing shutdown hook", e);
            }
        }));
    }

    protected abstract void startupServices(IMqttsnRuntimeRegistry runtime) throws MqttsnException;

    protected abstract void stopServices(IMqttsnRuntimeRegistry runtime) throws MqttsnException;

    protected final void callStartup(Object service) throws MqttsnException {
        if(service instanceof IMqttsnService){
            IMqttsnService snService =  (IMqttsnService) service;
            if(!snService.running()){
                logger.log(Level.INFO, String.format("starting [%s]", service.getClass().getName()));
                snService.start(registry);
            }
        }
    }

    protected final void callShutdown(Object service) throws MqttsnException {
        if(service instanceof IMqttsnService){
            IMqttsnService snService =  (IMqttsnService) service;
            if(snService.running()){
                logger.log(Level.INFO, String.format("stopping [%s]", service.getClass().getName()));
                snService.stop();
            }
        }
    }

    /**
     * Allow services to join the startup thread until startup is complete
     */
    public final void joinStartup() throws InterruptedException {
        startupLatch.await(60, TimeUnit.SECONDS);
    }

    protected void setupEnvironment(){
//        System.setProperty("java.util.logging.ConsoleHandler.level", "INFO");
//        System.setProperty("java.util.logging.ConsoleHandler.formatter", "java.util.logging.SimpleFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tc] %4$s %2$s - %5$s %6$s%n");
    }

    protected final void messageReceived(IMqttsnContext context, String topicName, int QoS, byte[] payload){
        logger.log(Level.FINE, String.format("publish received by application [%s]", topicName));
        receivedListeners.stream().forEach(p -> p.receive(context, topicName, QoS, payload));
    }

    protected final void messageSent(IMqttsnContext context, UUID messageId, String topicName, int QoS, byte[] payload){
        logger.log(Level.FINE, String.format("sent confirmed by application [%s]", topicName));
        sentListeners.stream().forEach(p -> p.sent(context, messageId, topicName, QoS, payload));
    }

    public void registerReceivedListener(IMqttsnPublishReceivedListener listener) {
        if(listener != null && !receivedListeners.contains(listener))
            receivedListeners.add(listener);
    }

    public void registerSentListener(IMqttsnPublishSentListener listener) {
        if(listener != null && !sentListeners.contains(listener))
            sentListeners.add(listener);
    }

    /**
     * A Disconnect was received from the remote context
     * @param context - The context who sent the DISCONNECT
     * @return should the local runtime send a DISCONNECT in reponse
     */
    public abstract boolean handleRemoteDisconnect(IMqttsnContext context);

    /**
     * When the runtime reaches a condition from which it cannot recover for the context,
     * it will generate a DISCONNECT to send to the context, the exception and context are then
     * passed to this method so the application has visibility of them
     * @param context - The context whose state encountered the problem thag caused the DISCONNECT
     * @param t - the exception that was encountered
     * @return was the exception handled, if so, the trace is not thrown up to the transport layer,
     * if not, the exception is reported into the transport layer
     */
    public abstract boolean handleLocalDisconnectError(IMqttsnContext context, Throwable t);

    /**
     * Submit work for the main worker thread group, this could be
     * transport operations or confirmations etc.
     */
    public Future<?> async(Runnable r){
        return executorService.submit(r);
    }
}