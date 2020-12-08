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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class QueuedPublishMessage implements Serializable {

    private String topicPath;
    private int grantedQoS;
    private int retryCount;
    private UUID messageId;
    private boolean retained;
    private Map<String, String> contextProperties = new HashMap<>();

    public QueuedPublishMessage() {
    }

    public QueuedPublishMessage(UUID messageId, String topicPath, int grantedQoS) {
        this.messageId = messageId;
        this.topicPath = topicPath;
        this.grantedQoS = grantedQoS;
        this.retryCount = 0;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public void setMessageId(UUID messageId) {
        this.messageId = messageId;
    }

    public boolean getRetained() {
        return retained;
    }

    public void setRetained(boolean retained) {
        this.retained = retained;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void incrementRetry(){
        retryCount++;
    }

    public String getTopicPath() {
        return topicPath;
    }

    public void setTopicPath(String topicPath) {
        this.topicPath = topicPath;
    }

    public int getGrantedQoS() {
        return grantedQoS;
    }

    public void setGrantedQoS(int grantedQoS) {
        this.grantedQoS = grantedQoS;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Map<String, String> getContextProperties() {
        return contextProperties;
    }

    public void setContextProperties(Map<String, String> contextProperties) {
        this.contextProperties = contextProperties;
    }

    public void addContextProperty(String key, String value){
        contextProperties.put(key, value);
    }

    @Override
    public String toString() {
        return "QueuedPublishMessage{" +
                "topicPath='" + topicPath + '\'' +
                ", grantedQoS=" + grantedQoS +
                ", retryCount=" + retryCount +
                ", messageId=" + messageId +
                ", retained=" + retained +
                ", contextProperties=" + contextProperties +
                '}';
    }
}
