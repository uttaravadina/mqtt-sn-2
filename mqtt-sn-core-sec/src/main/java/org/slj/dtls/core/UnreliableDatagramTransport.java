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
import java.util.Random;

import org.bouncycastle.tls.DatagramTransport;

public class UnreliableDatagramTransport
    implements DatagramTransport
{

    private final DatagramTransport transport;
    private final Random random;
    private final int percentPacketLossReceiving, percentPacketLossSending;

    public UnreliableDatagramTransport(DatagramTransport transport, Random random,
                                       int percentPacketLossReceiving, int percentPacketLossSending)
    {
        if (percentPacketLossReceiving < 0 || percentPacketLossReceiving > 100)
        {
            throw new IllegalArgumentException("'percentPacketLossReceiving' out of range");
        }
        if (percentPacketLossSending < 0 || percentPacketLossSending > 100)
        {
            throw new IllegalArgumentException("'percentPacketLossSending' out of range");
        }

        this.transport = transport;
        this.random = random;
        this.percentPacketLossReceiving = percentPacketLossReceiving;
        this.percentPacketLossSending = percentPacketLossSending;
    }

    public int getReceiveLimit()
        throws IOException
    {
        return transport.getReceiveLimit();
    }

    public int getSendLimit()
        throws IOException
    {
        return transport.getSendLimit();
    }

    public int receive(byte[] buf, int off, int len, int waitMillis)
        throws IOException
    {
        long endMillis = System.currentTimeMillis() + waitMillis;
        for (; ; )
        {
            int length = transport.receive(buf, off, len, waitMillis);
            if (length < 0 || !lostPacket(percentPacketLossReceiving))
            {
                return length;
            }

            System.out.println("PACKET LOSS (" + length + " byte packet not received)");

            long now = System.currentTimeMillis();
            if (now >= endMillis)
            {
                return -1;
            }

            waitMillis = (int)(endMillis - now);
        }
    }

    public void send(byte[] buf, int off, int len)
        throws IOException
    {
        if (lostPacket(percentPacketLossSending))
        {
            System.out.println("PACKET LOSS (" + len + " byte packet not sent)");
        }
        else
        {
            transport.send(buf, off, len);
        }
    }

    public void close()
        throws IOException
    {
        transport.close();
    }

    private boolean lostPacket(int percentPacketLoss)
    {
        return percentPacketLoss > 0 && random.nextInt(100) < percentPacketLoss;
    }
}
