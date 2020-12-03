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

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;

public class NetworkContext implements INetworkContext {

    protected IMqttsnContext mqttsnContext;
    protected NetworkAddress networkAddress;
    protected int receivePort;

    public NetworkContext(NetworkAddress networkAddress){
        this.networkAddress = networkAddress;
    }

    public NetworkContext(NetworkAddress networkAddress, IMqttsnContext mqttsnContext) {
        this.mqttsnContext = mqttsnContext;
        this.networkAddress = networkAddress;
    }

    public int getReceivePort() {
        return receivePort;
    }

    public void setReceivePort(int receivePort) {
        this.receivePort = receivePort;
    }

    @Override
    public IMqttsnContext getMqttsnContext() {
        return mqttsnContext;
    }

    public void setMqttsnContext(IMqttsnContext mqttsnContext) {
        this.mqttsnContext = mqttsnContext;
    }

    @Override
    public NetworkAddress getNetworkAddress() {
        return networkAddress;
    }

    public void setNetworkAddress(NetworkAddress networkAddress) {
        this.networkAddress = networkAddress;
    }



    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NetworkContext{");
        sb.append("mqttsnContext=").append(mqttsnContext);
        sb.append(", socketAddress=").append(networkAddress);
        sb.append(", receivePort=").append(receivePort);
        sb.append('}');
        return sb.toString();
    }
}
