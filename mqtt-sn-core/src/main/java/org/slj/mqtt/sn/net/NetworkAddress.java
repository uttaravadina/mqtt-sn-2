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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class NetworkAddress implements Serializable {

    private final String hostAddress;
    private final int port;
    private final String path;

    public NetworkAddress(int port, String hostAddress) throws UnknownHostException {
        this.hostAddress = InetAddress.getByName(hostAddress).getHostAddress();
        if(port < 0 || port > 65535) throw new IllegalArgumentException("port must be in range 0 <= port <= 65535");
        this.port = port;
        this.path = null;
    }

    public NetworkAddress(String path) {
        this.path = path;
        this.hostAddress = null;
        this.port = -1;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetworkAddress that = (NetworkAddress) o;
        return port == that.port &&
                Objects.equals(hostAddress, that.hostAddress) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostAddress, port, path);
    }

    public InetSocketAddress toSocketAddress(){
        return InetSocketAddress.createUnresolved(hostAddress, port);
    }

    public static NetworkAddress from(InetSocketAddress address) throws UnknownHostException {
        return NetworkAddress.from(address.getPort(), address.getAddress().getHostAddress());
    }

    public static NetworkAddress from(int port, String hostAddress) throws UnknownHostException {
        return new NetworkAddress(port, hostAddress);
    }

    public static NetworkAddress from(String path){
        return new NetworkAddress(path);
    }

    public static NetworkAddress localhost(int port) {
        try {
            return new NetworkAddress(port, "127.0.0.1");
        } catch(UnknownHostException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SocketAddress [");
        sb.append("hostAddress='").append(hostAddress).append('\'');
        sb.append(", port=").append(port);
        sb.append(", path='").append(path).append('\'');
        sb.append(']');
        return sb.toString();
    }
}
