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
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.IMqttsnService;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.model.MqttsnOptions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractMqttsnRuntime {

    protected Logger logger = Logger.getLogger(getClass().getName());
    protected IMqttsnRuntimeRegistry registry;
    protected CountDownLatch startupLatch, runtimeLatch;
    protected volatile boolean running = false;
    protected long startedAt;

    public final void start(IMqttsnRuntimeRegistry reg) throws MqttsnException {
        start(reg, false);
    }

    public final void start(IMqttsnRuntimeRegistry reg, boolean join) throws MqttsnException {
        if(runtimeLatch == null){
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
                    try {
                        Thread.sleep(1000);
                    } catch(InterruptedException e){
                        Thread.currentThread().interrupt();
                        throw new MqttsnException(e);
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

//        System.setProperty("java.util.logging.ConsoleHandler.formatter","org.slj.mqtt.sn.impl.MqttsnLogFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tc] %4$s %2$s - %5$s %6$s%n");
    }

    public void messageReceived(IMqttsnContext context, String topicName, int QoS, byte[] payload){
        logger.log(Level.INFO, String.format("publish received by application [%s]", topicName));
    }

    public void disconnectReceived(IMqttsnContext context){
        logger.log(Level.INFO, String.format("unsolicited disconnected received by application"));
    }
}