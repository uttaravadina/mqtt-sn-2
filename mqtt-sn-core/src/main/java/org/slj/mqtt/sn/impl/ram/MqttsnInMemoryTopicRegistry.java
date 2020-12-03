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

import org.slj.mqtt.sn.impl.AbstractTopicRegistry;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.MqttsnContext;
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MqttsnInMemoryTopicRegistry<T extends IMqttsnRuntimeRegistry>
        extends AbstractTopicRegistry<T> {

    protected Map<IMqttsnContext, Map<String, Integer>> topicLookups;

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        topicLookups = Collections.synchronizedMap(new HashMap<>());
    }

    protected Map<String, Integer> getRegistrations(IMqttsnContext context){
        Map<String, Integer> map = topicLookups.get(context);
        if(map == null){
            synchronized (this){
                if((map = topicLookups.get(context)) == null){
                    map = new HashMap<>();
                    topicLookups.put(context, map);
                }
            }
        }
        return map;
    }

    @Override
    protected boolean addOrUpdateRegistration(IMqttsnContext context, String topicPath, int alias) throws MqttsnException {

        Map<String, Integer> map = getRegistrations(context);
        return map.put(topicPath, alias) == null;
    }

    @Override
    protected Map<String, Integer> getPredefinedTopics(IMqttsnContext context) {
        return registry.getOptions().getPredefinedTopics();
    }

    @Override
    public void clearAll() throws MqttsnException {
        topicLookups.clear();
    }

    @Override
    public void clear(IMqttsnContext context) throws MqttsnException {
        topicLookups.remove(context);
    }
}
