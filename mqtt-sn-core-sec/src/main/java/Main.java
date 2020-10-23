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


import org.bouncycastle.tls.DTLSClientProtocol;
import org.bouncycastle.tls.DTLSServerProtocol;
import org.bouncycastle.tls.DTLSTransport;
import org.bouncycastle.tls.DatagramTransport;
import org.slj.dtls.core.*;

import java.security.SecureRandom;

public class Main {

    public static void main(String[] args) throws Exception {

        DTLSServerProtocol serverProtocol = new DTLSServerProtocol();
        MockDatagramAssociation datagramAssociation = new MockDatagramAssociation(1500);
        ServerThread serverThread = new ServerThread(serverProtocol,datagramAssociation.getServer());
        serverThread.start();

        SecureRandom secureRandom = new SecureRandom();

        DatagramTransport clientTransport = datagramAssociation.getClient();
        MqttsnDTLSClient client = new MqttsnDTLSClient(null);
        clientTransport = new UnreliableDatagramTransport(clientTransport, secureRandom, 0, 0);
        clientTransport = new LoggingDatagramTransport(clientTransport, System.out);
        DTLSClientProtocol clientProtocol = new DTLSClientProtocol();

        DTLSTransport dtlsClient = clientProtocol.connect(client, clientTransport);
        dtlsClient.send("test".getBytes(), 0, 11);
        byte[] buf = new byte[dtlsClient.getReceiveLimit()];
        while (dtlsClient.receive(buf, 0, buf.length, 100) >= 0) {
        }
        dtlsClient.close();
        serverThread.shutdown();

    }

    static class ServerThread
            extends Thread
    {
        private final DTLSServerProtocol serverProtocol;
        private final DatagramTransport serverTransport;
        private volatile boolean isShutdown = false;
        ServerThread(DTLSServerProtocol serverProtocol, DatagramTransport serverTransport)
        {
            this.serverProtocol = serverProtocol;
            this.serverTransport = serverTransport;
        }
        public void run()
        {
            try
            {
                System.out.println("server started");
                MqttsnDTLSServer server = new MqttsnDTLSServer();
                DTLSTransport dtlsServer = serverProtocol.accept(server, serverTransport);
                byte[] buf = new byte[dtlsServer.getReceiveLimit()];
                while (!isShutdown)
                {
                    int length = dtlsServer.receive(buf, 0, buf.length, 1000);
                    System.err.println("got " + length + " -> " + new String(buf));
//                    if (length >= 0)
//                    {
//                        dtlsServer.send(buf, 0, length);
//                    }
                }
                dtlsServer.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        void shutdown()
                throws InterruptedException
        {
            if (!isShutdown)
            {
                isShutdown = true;
                this.join();
            }
        }
    }
}
