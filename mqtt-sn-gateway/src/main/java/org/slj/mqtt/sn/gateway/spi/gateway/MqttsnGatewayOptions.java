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

package org.slj.mqtt.sn.gateway.spi.gateway;

import org.slj.mqtt.sn.model.MqttsnOptions;

import java.util.HashSet;
import java.util.Set;

public final class MqttsnGatewayOptions extends MqttsnOptions {

    public static final int DEFAULT_GATEWAY_ID = 5;
    public static final int DEFAULT_MAX_CONNECTED_CLIENTS = 10;
    public static final int DEFAULT_GATEWAY_ADVERTISE_TIME = 60;

    private Set<String> allowedClientIds = null;
    private int maxConnectedClients = DEFAULT_MAX_CONNECTED_CLIENTS;
    private int gatewayAdvertiseTime = DEFAULT_GATEWAY_ADVERTISE_TIME;
    private int gatewayId = DEFAULT_GATEWAY_ID;

    public MqttsnGatewayOptions withMaxConnectedClients(int maxConnectedClients){
        this.maxConnectedClients = maxConnectedClients;
        return this;
    }

    public MqttsnGatewayOptions withGatewayId(int gatewayId){
        this.gatewayId = gatewayId;
        return this;
    }

    public MqttsnGatewayOptions withGatewayAdvertiseTime(int gatewayAdvertiseTime){
        this.gatewayAdvertiseTime = gatewayAdvertiseTime;
        return this;
    }

    public int getGatewayAdvertiseTime() {
        return gatewayAdvertiseTime;
    }

    public int getGatewayId() {
        return gatewayId;
    }

    public int getMaxConnectedClients() {
        return maxConnectedClients;
    }

    public Set<String> getAllowedClientIds() {
        return allowedClientIds;
    }

    public MqttsnGatewayOptions withAllowedClientId(String clientId){
        if(allowedClientIds == null){
            synchronized (this) {
                if (allowedClientIds == null) {
                    allowedClientIds = new HashSet();
                }
            }
        }
        allowedClientIds.add(clientId);
        return this;
    }
}
