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

import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnConnect;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnPingreq;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class AbstractMqttsnTransport<U extends IMqttsnRuntimeRegistry>
        extends MqttsnService<U> implements IMqttsnTransport {

    protected ExecutorService executorService;

    @Override
    public void start(U runtime) throws MqttsnException {
        super.start(runtime);
        logger.log(Level.INFO, String.format("starting udp with options [%s]", System.identityHashCode(runtime.getOptions())));
        if(runtime.getOptions().getThreadHandoffFromTransport()){
            int threadCount = runtime.getOptions().getHandoffThreadCount();
            executorService =
                    Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
                        int count = 0;
                        ThreadGroup tg = new ThreadGroup("mqtt-sn-handoff");

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(tg, r, "mqtt-sn-handoff-thread-" + ++count);
                            t.setPriority(Thread.MIN_PRIORITY + 1);
                            return t;
                        }
                    });
             logger.log(Level.INFO, String.format("starting transport with [%s] handoff threads", threadCount));
        }
    }

    @Override
    public void stop() throws MqttsnException {
        super.stop();
        if(registry.getOptions().getThreadHandoffFromTransport()){
            try {
                if(!executorService.isShutdown()){
                    executorService.shutdown();
                }
                executorService.awaitTermination(30, TimeUnit.SECONDS);
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
            } finally {
                if(!executorService.isTerminated()){
                    executorService.shutdownNow();
                }
            }
        }
    }

    @Override
    public void receiveFromTransport(INetworkContext context, ByteBuffer buffer) {
        if(registry.getOptions().getThreadHandoffFromTransport()){
            executorService.submit(
                    () -> receiveFromTransportInternal(context, buffer));
        } else {
            receiveFromTransportInternal(context, buffer);
        }
    }

    protected void receiveFromTransportInternal(INetworkContext networkContext, ByteBuffer buffer) {
        try {
            byte[] data = drain(buffer);
            logger.log(Level.INFO, String.format("receiving [%s] bytes for [%s] from transport on thread [%s](%s)", data.length, networkContext,
                    Thread.currentThread().getName(), Thread.currentThread().getId()));
            IMqttsnMessage message = getRegistry().getCodec().decode(data);

            boolean authd = true;
            //-- if we detect an inbound id packet, we should authorise the context every time (even if the impl just reuses existing auth)
            if(message instanceof IMqttsnIdentificationPacket){
                if(networkContext.getMqttsnContext() == null){
                    authd = registry.getMessageHandler().authorizeContext(networkContext,
                            ((IMqttsnIdentificationPacket)message).getClientId());
                }
            }

            if(authd && networkContext.getMqttsnContext() != null){
                notifyTrafficReceived(networkContext, data, message);
                registry.getMessageHandler().receiveMessage(networkContext.getMqttsnContext(), message);
            } else {
                logger.log(Level.WARNING, "auth could not be established, send disconnect");
                writeToTransportInternal(networkContext, registry.getMessageFactory().createDisconnect(), false);
            }
        } catch(Throwable t){
            logger.log(Level.SEVERE, "error receiving message from transport", t);
        }
    }

    @Override
    public void writeToTransport(INetworkContext context, IMqttsnMessage message) {
        if(registry.getOptions().getThreadHandoffFromTransport()){
            executorService.submit(
                    () -> writeToTransportInternal(context, message, true));
        } else {
            writeToTransportInternal(context, message, true);
        }
    }

    protected void writeToTransportInternal(INetworkContext context, IMqttsnMessage message, boolean notifyListeners){
        try {
            byte[] data = registry.getCodec().encode(message);
            logger.log(Level.INFO, String.format("[%s] writing [%s] to transport on thread [%s](%s)", context, message,
                    Thread.currentThread().getName(), Thread.currentThread().getId()));
            writeToTransport(context, ByteBuffer.wrap(data, 0 , data.length));
            if(notifyListeners) notifyTrafficSent(context, data, message);
        } catch(Throwable e){
            logger.log(Level.SEVERE, String.format("[%s] transport layer errord sending buffer", context), e);
        }
    }

    private void notifyTrafficReceived(final INetworkContext context, byte[] data, IMqttsnMessage message) {
        List<IMqttsnTrafficListener> list = getRegistry().getTrafficListeners();
        if(list != null && !list.isEmpty()){
            list.stream().forEach(l -> l.trafficReceived(context, data, message));
        }
    }

    private void notifyTrafficSent(final INetworkContext context, byte[] data, IMqttsnMessage message) {
        List<IMqttsnTrafficListener> list = getRegistry().getTrafficListeners();
        if(list != null && !list.isEmpty()){
            list.stream().forEach(l -> l.trafficSent(context, data, message));
        }
    }

    protected abstract void writeToTransport(INetworkContext context, ByteBuffer data) throws MqttsnException ;

    protected static ByteBuffer wrap(byte[] arr){
        return wrap(arr, arr.length);
    }

    protected static ByteBuffer wrap(byte[] arr, int length){
        return ByteBuffer.wrap(arr, 0 , length);
    }

    protected static byte[] drain(ByteBuffer buffer){
        byte[] arr = new byte[buffer.remaining()];
        buffer.get(arr, 0, arr.length);
        return arr;
    }
}
