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
import java.io.PrintStream;

import org.bouncycastle.tls.DatagramTransport;
import org.bouncycastle.util.Strings;

public class LoggingDatagramTransport
    implements DatagramTransport
{

    private static final String HEX_CHARS = "0123456789ABCDEF";

    private final DatagramTransport transport;
    private final PrintStream output;
    private final long launchTimestamp;

    public LoggingDatagramTransport(DatagramTransport transport, PrintStream output)
    {
        this.transport = transport;
        this.output = output;
        this.launchTimestamp = System.currentTimeMillis();
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
        int length = transport.receive(buf, off, len, waitMillis);
        if (length >= 0)
        {
            dumpDatagram("Received", buf, off, length);
        }
        return length;
    }

    public void send(byte[] buf, int off, int len)
        throws IOException
    {
        dumpDatagram("Sending", buf, off, len);
        transport.send(buf, off, len);
    }

    public void close()
        throws IOException
    {
    }

    private void dumpDatagram(String verb, byte[] buf, int off, int len)
        throws IOException
    {
        long timestamp = System.currentTimeMillis() - launchTimestamp;
        StringBuffer sb = new StringBuffer("(+" + timestamp + "ms) " + verb + " " + len + " byte datagram:");
        for (int pos = 0; pos < len; ++pos)
        {
            if (pos % 16 == 0)
            {
                sb.append(Strings.lineSeparator());
                sb.append("    ");
            }
            else if (pos % 16 == 8)
            {
                sb.append('-');
            }
            else
            {
                sb.append(' ');
            }
            int val = buf[off + pos] & 0xFF;
            sb.append(HEX_CHARS.charAt(val >> 4));
            sb.append(HEX_CHARS.charAt(val & 0xF));
        }
        dump(sb.toString());
    }

    private synchronized void dump(String s)
    {
        output.println(s);
    }
}
