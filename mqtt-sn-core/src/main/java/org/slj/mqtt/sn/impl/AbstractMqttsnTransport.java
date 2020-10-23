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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class AbstractMqttsnTransport<U extends IMqttsnRuntimeRegistry>
        extends MqttsnService<U> implements IMqttsnTransport {

    protected ExecutorService executorService;

    @Override
    public void start(U runtime) throws MqttsnException {
        super.start(runtime);
        if(runtime.getOptions().getThreadHandoffFromTransport()){
            executorService = Executors.newSingleThreadExecutor();
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
        byte[] data = drain(buffer);
        logger.log(Level.INFO, "received " + MqttsnWireUtils.toBinary(data));
        IMqttsnMessage message = getRegistry().getCodec().decode(data);
        try {
            if(registry.getOptions().getThreadHandoffFromTransport()){
                executorService.submit(
                        () -> receiveFromTransport(context, message));
            } else {
                receiveFromTransport(context, message);
            }
        } finally {
            notifyTrafficReceived(context, data);
        }
    }

    private void notifyTrafficReceived(final INetworkContext context, byte[] data) {
        List<IMqttsnTrafficListener> list = getRegistry().getTrafficListeners();
        if(list != null && !list.isEmpty()){
            list.stream().forEach(l -> l.trafficReceived(context, data));
        }
    }

    private void notifyTrafficSent(final IMqttsnContext context, byte[] data) {
        List<IMqttsnTrafficListener> list = getRegistry().getTrafficListeners();
        if(list != null && !list.isEmpty()){
            list.stream().forEach(l -> l.trafficSent(context, data));
        }
    }

    protected void receiveFromTransport(INetworkContext context, IMqttsnMessage message) {
        try {
            logger.log(Level.INFO, String.format("[%s] handling receive buffer from transport on thread [%s](%s)", context,
                    Thread.currentThread().getName(), Thread.currentThread().getId()));
            if(context.getMqttsnContext() == null || message instanceof MqttsnConnect){
                IMqttsnContext mqttsnContext = registry.getMessageHandler().createContext(context, message);
                if(mqttsnContext != null){
                    context.setMqttsnContext(mqttsnContext);
                }
            }
            registry.getMessageHandler().receiveMessage(context.getMqttsnContext(), message);
        } catch(MqttsnException e){
            logger.log(Level.SEVERE, "error encountered receiving message from transport", e);
        }
    }

    @Override
    public void writeToTransport(IMqttsnContext context, IMqttsnMessage message) {
        byte[] arr = registry.getCodec().encode(message);
        try {
            if(registry.getOptions().getThreadHandoffFromTransport()){
                executorService.submit(
                        () -> writeToTransportInternal(context, ByteBuffer.wrap(arr, 0 , arr.length)));
            } else {
                writeToTransportInternal(context, ByteBuffer.wrap(arr, 0 , arr.length));
            }
        } finally {
            notifyTrafficSent(context, arr);
        }
    }

    protected void writeToTransportInternal(IMqttsnContext context, ByteBuffer buffer){
        try {
            logger.log(Level.INFO, String.format("[%s] writing buffer to transport on thread [%s](%s)", context,
                    Thread.currentThread().getName(), Thread.currentThread().getId()));
            writeToTransport(context, buffer);
        } catch(Exception e){
            logger.log(Level.SEVERE, String.format("[%s] transport layer errord sending buffer", context), e);
        }
    }

    protected abstract void writeToTransport(IMqttsnContext context, ByteBuffer data) throws MqttsnException ;

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
