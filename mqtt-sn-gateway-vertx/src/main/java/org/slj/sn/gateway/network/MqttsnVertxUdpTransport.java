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

package org.slj.sn.gateway.network;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocket;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnUdpTransport;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.net.NetworkContext;
import org.slj.mqtt.sn.spi.NetworkRegistryException;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;

import java.nio.ByteBuffer;
import java.util.logging.Level;

public class MqttsnVertxUdpTransport
        extends AbstractMqttsnUdpTransport<IMqttsnGatewayRuntimeRegistry> {

    protected final DatagramSocket socket;

    public MqttsnVertxUdpTransport(MqttsnUdpOptions options, DatagramSocket socket){
        super(options);
        this.socket = socket;
    }

    protected void bind(){
        socket.listen(options.getPort(), options.getHost(), asyncResult -> {
            if (asyncResult.succeeded()) {
                socket.handler(packet -> {
                    try {
                        String host = packet.sender().host();
                        int port = packet.sender().port();
                        logger.log(Level.INFO,
                                String.format("incoming packet detected from host [%s] on port [%s];", host, port));
                        NetworkAddress address = NetworkAddress.from(port, host);
                        INetworkContext networkContext =
                                registry.getNetworkRegistry().getContext(address);
                        if(networkContext == null){
                            networkContext = new NetworkContext(address, null);
                        }
                        byte[] arr = packet.data().getBytes();
                        if(logger.isLoggable(Level.INFO)){
                            logger.log(Level.INFO,
                                    String.format("received [%s] bytes from [%s] -> [%s]", arr.length, networkContext, MqttsnWireUtils.toBinary(arr)));
                        }
                        receiveFromTransport(networkContext, ByteBuffer.wrap(arr, 0 , arr.length));
                    } catch(Exception  e){
                        logger.log(Level.SEVERE, "error occurred processing receiving message;", e);
                    }
                });
            } else {

                logger.log(Level.SEVERE, "error occurred in transport", asyncResult.cause());
            }
        });
    }

    @Override
    public void writeToTransport(IMqttsnContext context, ByteBuffer buffer) {
        try {
            NetworkAddress address = registry.getNetworkRegistry().getNetworkAddress(context);
            if(address != null){
                byte[] bb = drain(buffer);
                Buffer buf = Buffer.buffer(bb);
                socket.send(buf, address.getPort(), address.getHostAddress(), asyncResult -> {
                    if(logger.isLoggable(Level.INFO)){
                        logger.log(Level.INFO,
                                String.format("sent [%s] bytes to [%s] -> [%s]", bb.length, context, MqttsnWireUtils.toBinary(bb)));
                    }
                });
            }
        } catch(NetworkRegistryException e){
            logger.log(Level.SEVERE,
                    String.format("network registry error"), e);
        }
    }

    @Override
    public void broadcast(IMqttsnMessage message) throws MqttsnException {
        throw new UnsupportedOperationException("TODO bind discovery into vert.x");
    }
}