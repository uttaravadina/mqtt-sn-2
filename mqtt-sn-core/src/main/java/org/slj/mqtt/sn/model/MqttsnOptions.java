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

package org.slj.mqtt.sn.model;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.spi.MqttsnRuntimeException;
import org.slj.mqtt.sn.utils.TopicPath;

import java.util.HashMap;
import java.util.Map;

public class MqttsnOptions {

    public static final boolean DEFAULT_BLOCK_SEND_ON_PREVIOUS_CONFIRMATION = true;
    public static final boolean DEFAULT_DISCOVERY_ENABLED = false;
    public static final boolean DEFAULT_THREAD_HANDOFF_ENABLED = true;
    public static final int DEFAULT_MAX_TOPICS_IN_REGISTRY = 128;
    public static final int DEFAULT_MSG_ID_STARTS_AT = 1;
    public static final int DEFAULT_ALIAS_STARTS_AT = 1;
    public static final int DEFAULT_MAX_MESSAGES_IN_FLIGHT = 2;
    public static final int DEFAULT_MAX_MESSAGE_IN_QUEUE = 100;
    public static final boolean DEFAULT_REQUEUE_ON_INFLIGHT_TIMEOUT = true;
    public static final int DEFAULT_MAX_TOPIC_LENGTH = 1024;
    public static final int DEFAULT_MAX_NETWORK_ADDRESS_ENTRIES = 1024;
    public static final int DEFAULT_MAX_WAIT = 10000;
    public static final int DEFAULT_MAX_TIME_INFLIGHT = 30000;
    public static final int DEFAULT_MIN_FLUSH_TIME = 1000;
    public static final int DEFAULT_SEARCH_GATEWAY_RADIUS = 2;

    private String contextId;
    private boolean blockSendOnPreviousConfirmation = DEFAULT_BLOCK_SEND_ON_PREVIOUS_CONFIRMATION;
    private boolean threadHandoffFromTransport = DEFAULT_THREAD_HANDOFF_ENABLED;
    private boolean enableDiscovery = DEFAULT_DISCOVERY_ENABLED;
    private int minFlushTime = DEFAULT_MIN_FLUSH_TIME;
    private int maxTopicsInRegistry = DEFAULT_MAX_TOPICS_IN_REGISTRY;
    private int msgIdStartAt = DEFAULT_MSG_ID_STARTS_AT;
    private int aliasStartAt = DEFAULT_ALIAS_STARTS_AT;
    private int maxMessagesInflight = DEFAULT_MAX_MESSAGES_IN_FLIGHT;
    private int maxMessagesInQueue = DEFAULT_MAX_MESSAGE_IN_QUEUE;
    private boolean requeueOnInflightTimeout = DEFAULT_REQUEUE_ON_INFLIGHT_TIMEOUT;
    private int maxTopicLength = DEFAULT_MAX_TOPIC_LENGTH;
    private int maxNetworkAddressEntries = DEFAULT_MAX_NETWORK_ADDRESS_ENTRIES;
    private int maxWait = DEFAULT_MAX_WAIT;
    private int maxTimeInflight = DEFAULT_MAX_TIME_INFLIGHT;
    private int searchGatewayRadius = DEFAULT_SEARCH_GATEWAY_RADIUS;

    private Map<String, Integer> predefinedTopics;
    private Map<String, NetworkAddress> networkAddressEntries;

    public MqttsnOptions withMinFlushTime(int minFlushTime){
        this.minFlushTime = minFlushTime;
        return this;
    }

    public MqttsnOptions withSearchGatewayRadius(int searchGatewayRadius){
        this.searchGatewayRadius = searchGatewayRadius;
        return this;
    }

    public MqttsnOptions withThreadHandoffFromTransport(boolean threadHandoffFromTransport){
        this.threadHandoffFromTransport = threadHandoffFromTransport;
        return this;
    }

    public MqttsnOptions withBlockSendOnPreviousConfirmation(boolean blockSendOnPreviousConfirmation){
        this.blockSendOnPreviousConfirmation = blockSendOnPreviousConfirmation;
        return this;
    }

    public MqttsnOptions withMaxMessagesInQueue(int maxMessagesInQueue){
        this.maxMessagesInQueue = maxMessagesInQueue;
        return this;
    }

    public MqttsnOptions withRequeueOnInflightTimeout(boolean requeueOnInflightTimeout){
        this.requeueOnInflightTimeout = requeueOnInflightTimeout;
        return this;
    }

    public MqttsnOptions withMaxWait(int maxWait){
        this.maxWait = maxWait;
        return this;
    }

    public MqttsnOptions withMaxTimeInflight(int maxTimeInflight){
        this.maxTimeInflight = maxTimeInflight;
        return this;
    }

    public MqttsnOptions withMaxMessagesInflight(int maxMessagesInflight){
        this.maxMessagesInflight = maxMessagesInflight;
        return this;
    }

    public MqttsnOptions withPredefinedTopic(String topicPath, int alias){
        if(!TopicPath.isValidTopic(topicPath, Math.max(maxTopicLength, MqttsnConstants.USIGNED_MAX_16))){
            throw new MqttsnRuntimeException("invalid topic path " + topicPath);
        }

        if(predefinedTopics == null){
            synchronized (this) {
                if (predefinedTopics == null) {
                    predefinedTopics = new HashMap();
                }
            }
        }
        predefinedTopics.put(topicPath, alias);
        return this;
    }

    public MqttsnOptions withMaxTopicLength(int maxTopicLength){
        this.maxTopicLength = maxTopicLength;
        return this;
    }

    public MqttsnOptions withMsgIdsStartAt(int msgIdStartAt){
        this.msgIdStartAt = msgIdStartAt;
        return this;
    }

    public MqttsnOptions withMaxTopicsInRegistry(int maxTopicsInRegistry){
        this.maxTopicsInRegistry = maxTopicsInRegistry;
        return this;
    }

    public MqttsnOptions withDiscoveryEnabled(boolean enableDiscovery){
        this.enableDiscovery = enableDiscovery;
        return this;
    }

    public MqttsnOptions withMaxNetworkAddressEntries(int maxNetworkAddressEntries){
        this.maxNetworkAddressEntries = maxNetworkAddressEntries;
        return this;
    }

    public MqttsnOptions withAliasStartAt(int aliasStartAt){
        this.aliasStartAt = aliasStartAt;
        return this;
    }

    public MqttsnOptions withContextId(String contextId){
        this.contextId = contextId;
        return this;
    }

    public MqttsnOptions withNetworkAddressEntry(String gatewayId, NetworkAddress address){
        if(networkAddressEntries == null){
            synchronized (this) {
                if (networkAddressEntries == null) {
                    networkAddressEntries = new HashMap();
                }
            }
        }
        networkAddressEntries.put(gatewayId, address);
        return this;
    }

    public Map<String, NetworkAddress> getNetworkAddressEntries() {
        return networkAddressEntries;
    }

    public int getAliasStartAt() {
        return aliasStartAt;
    }

    public int getMsgIdStartAt() {
        return msgIdStartAt;
    }

    public int getMaxTopicsInRegistry() {
        return maxTopicsInRegistry;
    }

    public boolean isEnableDiscovery() {
        return enableDiscovery;
    }

    public int getMaxTopicLength() {
        return maxTopicLength;
    }

    public String getContextId() {
        return contextId;
    }

    public int getMaxTimeInflight() {
        return maxTimeInflight;
    }

    public int getMaxNetworkAddressEntries() {
        return maxNetworkAddressEntries;
    }

    public Map<String, Integer> getPredefinedTopics() {
        return predefinedTopics;
    }

    public boolean getBlockSendOnPreviousConfirmation() {
        return blockSendOnPreviousConfirmation;
    }

    public int getMaxMessagesInflight() {
        return maxMessagesInflight;
    }

    public boolean getRequeueOnInflightTimeout() {
        return requeueOnInflightTimeout;
    }

    public int getMaxMessagesInQueue() {
        return maxMessagesInQueue;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public boolean getThreadHandoffFromTransport() {
        return threadHandoffFromTransport;
    }

    public int getSearchGatewayRadius() {
        return searchGatewayRadius;
    }

    public int getMinFlushTime() {
        return minFlushTime;
    }
}
