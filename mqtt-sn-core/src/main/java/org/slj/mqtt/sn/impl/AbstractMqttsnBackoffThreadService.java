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

import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.MqttsnService;

import java.util.logging.Level;

public abstract class AbstractMqttsnBackoffThreadService<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T> implements Runnable {

    static final int DEFAULT_BACKOFF_FACTOR = 500;
    static final int MAX_BACKOFF_INCR = 10;
    private Thread t;
    private volatile boolean stopped = false;
    private Object monitor = new Object();

    @Override
    public synchronized void start(T runtime) throws MqttsnException {
        super.start(runtime);
        initThread();
    }

    protected void initThread(){
        if(t == null){
            String threadName = String.format("mqtt-sn-deamon-%s", getClass().getSimpleName().toLowerCase());
            t = new Thread(this, threadName);
            t.setPriority(Thread.MIN_PRIORITY);
            t.setDaemon(true);
            t.start();
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.log(Level.SEVERE, "uncaught error on deamon process;", e);
                }
            });
        }
    }

    @Override
    public void stop() throws MqttsnException {
        super.stop();
        t = null;
    }

    @Override
    public final void run() {
        int count = 1;
        try {
            registry.getRuntime().joinStartup();
        } catch(Exception e){
            Thread.currentThread().interrupt();
            throw new RuntimeException("error joining startup", e);
        }

        logger.log(Level.INFO, String.format("starting thread [%s] processing", Thread.currentThread().getName()));
        while(running &&
                !Thread.currentThread().isInterrupted()){
            try {
                if(doWork()){
                    //when the execute returns true, reset the backoff
                    count = 0;
                }
                long backoff = (long) Math.pow(2, Math.min(count++, MAX_BACKOFF_INCR)) * getBackoffFactor();
                if(logger.isLoggable(Level.FINE)){
                    logger.log(Level.FINE,
                            String.format("processing [%s] on count [%s] sleeping for [%s]", Thread.currentThread().getName(), count, backoff));
                }
                synchronized (monitor){
                    monitor.wait(backoff);
                }
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        logger.log(Level.INFO, String.format("stopped %s thread", Thread.currentThread().getName()));
    }

    protected int getBackoffFactor(){
        return DEFAULT_BACKOFF_FACTOR;
    }

    /**
     * Complete your tasks in this method.
     * WARNING, throwing an unchecked exception from this method will cause the service to shutdown
     */
    protected abstract boolean doWork();

    protected void expedite(){
        synchronized (monitor){
            monitor.notifyAll();
        }
    }
}
