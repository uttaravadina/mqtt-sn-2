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

import org.bouncycastle.tls.DTLSServerProtocol;
import org.bouncycastle.tls.DTLSTransport;
import org.bouncycastle.tls.DatagramTransport;
import org.slj.dtls.core.MqttsnDTLSServer;
import org.slj.dtls.core.UnreliableDatagramTransport;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.model.NetworkContext;
import org.slj.mqtt.sn.net.MqttsnUdpOptions;
import org.slj.mqtt.sn.net.MqttsnUdpTransport;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

public class MqttsnDTLSTransport extends MqttsnUdpTransport {

    private DatagramSocket dtlsSocket;
    private Thread dtslReceiverThread;

    public MqttsnDTLSTransport(MqttsnUdpOptions udpOptions){
        super(udpOptions);
    }

    protected void bind() throws SocketException {

        //start the plain UDP socket
        super.bind();

        //start the secure socket
        dtlsSocket = options.getSecurePort() > 0 ? new DatagramSocket(options.getSecurePort()) : new DatagramSocket();
        dtslReceiverThread = createDatagramServer("mqttsn-dtls-receiver", options.getReceiveBuffer(), dtlsSocket);
    }

    public void writeToTransport(IMqttsnContext context, ByteBuffer buffer) throws MqttsnException {

        //if we receive thru the secure port, we should write thru the secure port
        INetworkContext networkContext = registry.getNetworkRegistry().getContext(context);
        if(networkContext.getReceivePort() == options.getSecurePort()){

            //-- use the DTLS version...

        } else {
            super.writeToTransport(context, buffer);
        }
    }


    protected Thread createDTLSServer(String threadName, int bufferSize){

        final DTLSServerProtocol serverProtocol = new DTLSServerProtocol();
        final DatagramTransport serverTransport;

        Thread thread = new Thread(() -> {
            logger.log(Level.INFO, String.format("creating dtls server [%s] bound to socket [%s] with buffer size [%s], running ? [%s]", threadName, localSocket.getLocalPort(), bufSize, running));
            byte[] buff = new byte[bufferSize];
            MqttsnDTLSServer server = new MqttsnDTLSServer();

            UnreliableDatagramTransport udp = null;//new UnreliableDatagramTransport();
            DTLSTransport dtlsServer = null;//serverProtocol.accept(server, new DatagramTransport(){

//            });

            while(running){
                try {
                    byte[] buf = new byte[dtlsServer.getReceiveLimit()];
                    dtlsServer.receive(buff, 0, buff.length, bufferSize);

                    NetworkAddress address = NetworkAddress.from(p.getPort(), p.getAddress().getHostAddress());
                    INetworkContext context = registry.getNetworkRegistry().getContext(address);
                    if(context == null){
                        logger.log(Level.INFO, String.format("creating new [%s] context for [%s]", threadName, address));
                        context = new NetworkContext(address, null);
                    }
                    context.setReceivePort(localSocket.getLocalPort());
                    receiveFromTransport(context, wrap(buff, p.getLength()));
                } catch(Exception e){
                    logger.log(Level.SEVERE, "encountered an error listening for traffic", e);
                } finally {
                    buff = new byte[bufferSize];
                }
            }
        }, threadName);
        thread.setDaemon(true);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.start();
        return thread;
    }

}