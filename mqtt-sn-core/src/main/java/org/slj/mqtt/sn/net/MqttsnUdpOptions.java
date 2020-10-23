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

public class MqttsnUdpOptions {

    public static String DEFAULT_LOCAL_BIND_INTERFACE = "localhost";
    public static int DEFAULT_LOCAL_PORT = 2442;
    public static int DEFAULT_SECURE_PORT = 2443;
    public static int DEFAULT_MULTICAST_PORT = 2224;
    public static int DEFAULT_RECEIVE_BUFFER_SIZE = 1024;
    public static long DEFAULT_MAIN_LOOP_BACKOFF = 50;
    public static boolean DEFAULT_BIND_BROADCAST_LISTENER = false;

    String host = DEFAULT_LOCAL_BIND_INTERFACE;
    int port = DEFAULT_LOCAL_PORT;
    int securePort = DEFAULT_SECURE_PORT;
    int multicastPort = DEFAULT_MULTICAST_PORT;
    int receiveBuffer = DEFAULT_RECEIVE_BUFFER_SIZE;
    boolean bindBroadcastListener = DEFAULT_BIND_BROADCAST_LISTENER;
    long mainLoopBackoff = DEFAULT_MAIN_LOOP_BACKOFF;

    public long getMainLoopBackoff() {
        return mainLoopBackoff;
    }

    public void setMainLoopBackoff(int mainLoopBackoff) {
        this.mainLoopBackoff = mainLoopBackoff;
    }

    public boolean getBindBroadcastListener() {
        return bindBroadcastListener;
    }

    public void setBindBroadcastListener(boolean bindBroadcastListener) {
        this.bindBroadcastListener = bindBroadcastListener;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getReceiveBuffer() {
        return receiveBuffer;
    }

    public int getMulticastPort(){ return this.multicastPort; }

    public MqttsnUdpOptions withMulticastPort(int multicastPort){
        this.multicastPort = multicastPort;
        return this;
    }

    public MqttsnUdpOptions withPort(int port){
        this.port = port;
        return this;
    }

    public MqttsnUdpOptions withHost(String host){
        this.host = host;
        return this;
    }

    public MqttsnUdpOptions withSecurePort(int port){
        this.securePort = port;
        return this;
    }

    public int getSecurePort() {
        return securePort;
    }

    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }

    public void setReceiveBuffer(int receiveBuffer) {
        this.receiveBuffer = receiveBuffer;
    }

    public void setMainLoopBackoff(long mainLoopBackoff) {
        this.mainLoopBackoff = mainLoopBackoff;
    }

    public MqttsnUdpOptions withMainLoopBackoff(long mainLoopBackoff){
        this.mainLoopBackoff = mainLoopBackoff;
        return this;
    }

    public MqttsnUdpOptions withReceiveBuffer(int receiveBuffer){
        this.receiveBuffer = receiveBuffer;
        return this;
    }

    public MqttsnUdpOptions withBindBroadcastListener(boolean broadcastListener){
        this.bindBroadcastListener = broadcastListener;
        return this;
    }
}
