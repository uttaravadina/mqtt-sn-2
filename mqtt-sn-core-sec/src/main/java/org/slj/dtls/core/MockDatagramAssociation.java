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

package org.slj.dtls.core;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.Vector;

import org.bouncycastle.tls.DatagramTransport;

public class MockDatagramAssociation
{
    private int mtu;
    private MockDatagramTransport client, server;

    public MockDatagramAssociation(int mtu)
    {
        this.mtu = mtu;

        Vector clientQueue = new Vector();
        Vector serverQueue = new Vector();

        this.client = new MockDatagramTransport(clientQueue, serverQueue);
        this.server = new MockDatagramTransport(serverQueue, clientQueue);
    }

    public DatagramTransport getClient()
    {
        return client;
    }

    public DatagramTransport getServer()
    {
        return server;
    }

    private class MockDatagramTransport
        implements DatagramTransport
    {
        private Vector receiveQueue, sendQueue;

        MockDatagramTransport(Vector receiveQueue, Vector sendQueue)
        {
            this.receiveQueue = receiveQueue;
            this.sendQueue = sendQueue;
        }

        public int getReceiveLimit()
            throws IOException
        {
            return mtu;
        }

        public int getSendLimit()
            throws IOException
        {
            return mtu;
        }

        public int receive(byte[] buf, int off, int len, int waitMillis)
            throws IOException
        {
            synchronized (receiveQueue)
            {
                if (receiveQueue.isEmpty())
                {
                    try
                    {
                        receiveQueue.wait(waitMillis);
                    }
                    catch (InterruptedException e)
                    {
                        // TODO Keep waiting until full wait expired?
                    }
                    if (receiveQueue.isEmpty())
                    {
                        return -1;
                    }
                }
                DatagramPacket packet = (DatagramPacket)receiveQueue.remove(0);
                int copyLength = Math.min(len, packet.getLength());
                System.arraycopy(packet.getData(), packet.getOffset(), buf, off, copyLength);
                return copyLength;
            }
        }

        public void send(byte[] buf, int off, int len)
            throws IOException
        {
            if (len > mtu)
            {
                // TODO Simulate rejection?
            }

            byte[] copy = new byte[len];
            System.arraycopy(buf, off, copy, 0, len);
            DatagramPacket packet = new DatagramPacket(copy, len);

            synchronized (sendQueue)
            {
                sendQueue.addElement(packet);
                sendQueue.notify();
            }
        }

        public void close()
            throws IOException
        {
            // TODO?
        }
    }
}
