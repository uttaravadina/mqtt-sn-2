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

package org.slj.mqtt.sn.net;

import org.slj.mqtt.sn.impl.AbstractMqttsnUdpTransport;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;

public class MqttsnUdpTransport extends AbstractMqttsnUdpTransport {

    private DatagramSocket socket;
    private DatagramSocket broadcastSocket;
    private Thread receiverThread;
    private Thread broadcastThread;
    protected boolean running = true;

    public MqttsnUdpTransport(MqttsnUdpOptions udpOptions){
        super(udpOptions);
    }

    protected void bind() throws SocketException {

        int bufferSize = options.getReceiveBuffer();
        socket = options.getPort() > 0 ? new DatagramSocket(options.getPort()) : new DatagramSocket();
        receiverThread = createDatagramServer("mqttsn-udp-receiver", bufferSize, socket);
        if(options.getBindBroadcastListener() && registry.getOptions().isEnableDiscovery()) {
            broadcastSocket = options.getMulticastPort() > 0 ? new DatagramSocket(options.getMulticastPort()) : new DatagramSocket();
            broadcastSocket.setBroadcast(true);
            broadcastThread = createDatagramServer("mqttsn-udp-broadcast", bufferSize, broadcastSocket);
        }
    }

    protected Thread createDatagramServer(final String threadName, final int bufSize, final DatagramSocket localSocket){
        Thread thread = new Thread(() -> {
            logger.log(Level.INFO, String.format("creating udp server [%s] bound to socket [%s] with buffer size [%s], running ? [%s]", threadName, localSocket.getLocalPort(), bufSize, running));
            byte[] buff = new byte[bufSize];
            while(running){
                try {
                    DatagramPacket p = new DatagramPacket(buff, buff.length);
                    localSocket.receive(p);
                    NetworkAddress address = NetworkAddress.from(p.getPort(), p.getAddress().getHostAddress());
                    INetworkContext context = registry.getNetworkRegistry().getContext(address);
                    if(context == null){
                        logger.log(Level.FINE, String.format("creating new [%s] context for [%s]", threadName, address));
                        context = new NetworkContext(address, null);
                    }
                    context.setReceivePort(localSocket.getLocalPort());
                    receiveFromTransport(context, wrap(buff, p.getLength()));
                } catch(Exception e){
                    logger.log(Level.SEVERE, "encountered an error listening for traffic", e);
                } finally {
                    buff = new byte[bufSize];
                }
            }
        }, threadName);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
        return thread;
    }

    @Override
    public void stop() throws MqttsnException {
        super.stop();
        running = false;
        socket = null;
        receiverThread = null;
        broadcastThread = null;
    }

    @Override
    public void writeToTransport(IMqttsnContext context, ByteBuffer buffer) throws MqttsnException {
        try {
            byte[] payload = drain(buffer);
            NetworkAddress address = context.getNetworkContext().getNetworkAddress();
            InetAddress inetAddress = InetAddress.getByName(address.getHostAddress());
            DatagramPacket packet = new DatagramPacket(payload, payload.length, inetAddress, address.getPort());
            logger.log(Level.FINE, String.format("writing [%s] bytes to [%s] port [%s]", payload.length, inetAddress, address.getPort()));
            socket.send(packet);
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }

    @Override
    public void broadcast(IMqttsnMessage broadcastMessage) throws MqttsnException {
        try {
            byte[] arr = registry.getCodec().encode(broadcastMessage);
            List<InetAddress> broadcastAddresses = registry.getNetworkRegistry().getAllBroadcastAddresses();
            try (DatagramSocket socket = new DatagramSocket()){
                socket.setBroadcast(true);
                for(InetAddress address : broadcastAddresses) {
                    logger.log(Level.FINE, String.format("broadcasting [%s] message to network interface [%s] -> [%s]",
                            broadcastMessage.getMessageName(), address, options.getMulticastPort()));
                    DatagramPacket packet
                            = new DatagramPacket(arr, arr.length, address, options.getMulticastPort());
                    socket.send(packet);
                }
            }
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }
}
