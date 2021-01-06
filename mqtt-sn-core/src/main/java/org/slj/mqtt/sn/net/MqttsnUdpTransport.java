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

/**
 * Provides a transport over User Datagram Protocol (UDP). This implementation uses a receiver thread which binds
 * onto a Socket in a tight loop blocking on receive with no socket timeout set (0). The receiver thread will simply hand packets
 * off to the base receive method who will either pass to the thread pool for handling or handle blocking depending on the
 * configuration of the runtime.
 *
 * The broadcast-receiver when activated runs on it own thread, listening on the broadcast port, updating the registry
 * when new contexts are discovered.
 */
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
        //-- by default we do not set SoTimeout (infinite) which will block until recieve
        receiverThread = createDatagramServer("mqttsn-udp-receiver", bufferSize, socket);
        if(options.getBindBroadcastListener() && registry.getOptions().isEnableDiscovery()) {
            broadcastSocket = options.getBroadcastPort() > 0 ? new DatagramSocket(options.getBroadcastPort()) : new DatagramSocket();
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
                        //-- if the network context does not exist in the registry, a new one is created by the factory -
                        //- NB: this is NOT auth, this is simply creating a context to which we can respond, auth can
                        //-- happen during the mqtt-sn context creation, at which point we can talk back to the device
                        //-- with error packets and the like
                        context = registry.getContextFactory().createInitialNetworkContext(address);
                    }
                    context.setReceivePort(localSocket.getLocalPort());
                    receiveFromTransport(context, wrap(buff, p.getLength()));
                } catch(Throwable e){
                    logger.log(Level.SEVERE, "encountered an error listening for traffic", e);
                } finally {
                    buff = new byte[bufSize];
                }
            }
        }, threadName);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY + 1);
        thread.start();
        return thread;
    }

    @Override
    public void stop() throws MqttsnException {
        super.stop();
        running = false;
        socket = null;
        broadcastSocket = null;
        receiverThread = null;
        broadcastThread = null;
    }

    @Override
    public void writeToTransport(INetworkContext context, ByteBuffer buffer) throws MqttsnException {
        try {
            byte[] payload = drain(buffer);
            NetworkAddress address = context.getNetworkAddress();
            InetAddress inetAddress = InetAddress.getByName(address.getHostAddress());
            DatagramPacket packet = new DatagramPacket(payload, payload.length, inetAddress, address.getPort());
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
                            broadcastMessage.getMessageName(), address, options.getBroadcastPort()));
                    DatagramPacket packet
                            = new DatagramPacket(arr, arr.length, address, options.getBroadcastPort());
                    socket.send(packet);
                }
            }
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }
}
