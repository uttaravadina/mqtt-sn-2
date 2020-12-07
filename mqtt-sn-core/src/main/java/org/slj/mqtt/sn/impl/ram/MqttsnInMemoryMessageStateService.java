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

package org.slj.mqtt.sn.impl.ram;

import org.slj.mqtt.sn.impl.AbstractMqttsnMessageStateService;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.InflightMessage;
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

public class MqttsnInMemoryMessageStateService <T extends IMqttsnRuntimeRegistry>
        extends AbstractMqttsnMessageStateService<T> {

    protected Map<IMqttsnContext, Map<Integer, InflightMessage>> inflightMessages;

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        inflightMessages = Collections.synchronizedMap(new HashMap());
    }

    @Override
    protected boolean doWork() {
        Iterator<IMqttsnContext> itr = inflightMessages.keySet().iterator();
        synchronized (inflightMessages) {
            while (itr.hasNext()) {
                try {
                    IMqttsnContext context = itr.next();
                    clearInflight(context, System.currentTimeMillis());
                } catch(MqttsnException e){
                    logger.log(Level.WARNING, "error occurred during inflight eviction run;", e);
                }
            }
        }
        return super.doWork();
    }

    @Override
    public void clear(IMqttsnContext context) {
        inflightMessages.remove(context);
    }

    @Override
    public void clearAll() {
        inflightMessages.clear();
    }

    @Override
    protected InflightMessage removeInflightMessage(IMqttsnContext context, Integer messageId) throws MqttsnException {
        return getInflightMessages(context).remove(messageId);
    }

    @Override
    protected void addInflightMessage(IMqttsnContext context, Integer messageId, InflightMessage message) throws MqttsnException {
        getInflightMessages(context).put(messageId, message);
    }

    @Override
    protected Map<Integer, InflightMessage> getInflightMessages(IMqttsnContext context) throws MqttsnException {
        Map<Integer, InflightMessage> map = inflightMessages.get(context);
        if(map == null){
            synchronized (this){
                if((map = inflightMessages.get(context)) == null){
                    map = new HashMap<>();
                    inflightMessages.put(context, map);
                }
            }
        }
        return map;
    }
}