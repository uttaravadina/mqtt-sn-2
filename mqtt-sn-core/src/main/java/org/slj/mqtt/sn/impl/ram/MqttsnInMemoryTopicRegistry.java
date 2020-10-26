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

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

public class MqttsnInMemoryTopicRegistry<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T>
        implements IMqttsnTopicRegistry<T> {

    protected Map<IMqttsnContext, Map<String, Integer>> topicLookups;

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        topicLookups = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public TopicInfo register(IMqttsnContext context, String topicAlias) throws MqttsnException {
        Map<String, Integer> map = getLookup(context);
        if(map.size() >= registry.getOptions().getMaxTopicsInRegistry()){
            logger.log(Level.WARNING, String.format("max number of registered topics reached for client [%s] >= [%s]", context, map.size()));
            throw new MqttsnException("max number of registered topics reached for client");
        }
        synchronized (context){
            int alias = MqttsnUtils.getNextLeaseId(map.values(), Math.max(1, registry.getOptions().getAliasStartAt()));
            map.put(topicAlias, alias);
            TopicInfo info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL,  alias);
            return info;
        }
    }

    @Override
    public void register(IMqttsnContext context, String topicPath, int topicAlias) throws MqttsnException {
        Map<String, Integer> map = getLookup(context);
        if(map.containsKey(topicPath)){
            //update existing
            map.put(topicPath, topicAlias);
        } else {
            if(map.size() >= registry.getOptions().getMaxTopicsInRegistry()){
                logger.log(Level.WARNING, String.format("max number of registered topics reached for client [%s] >= [%s]", context, map.size()));
                throw new MqttsnException("max number of registered topics reached for client");
            }
            map.put(topicPath, topicAlias);
        }
    }

    @Override
    public boolean registered(IMqttsnContext context, String topicPath) throws MqttsnException {
        if(topicLookups.containsKey(context)){
            Map<String, Integer> map = getLookup(context);
            return map.containsKey(topicPath);
        }
        return false;
    }

    @Override
    public String topicPath(IMqttsnContext context, TopicInfo topicInfo, boolean considerContext) throws MqttsnException {
        String topicPath = null;
        switch (topicInfo.getType()){
            case SHORT:
                topicPath = topicInfo.getTopicPath();
                break;
            case PREDEFINED:
                topicPath = lookupPredefined(topicInfo.getTopicId());
                break;
            case NORMAL:
                if(considerContext){
                    if(context == null) throw new MqttsnExpectationFailedException("<null> context cannot be considered");
                    topicPath = lookupRegistered(context, topicInfo.getTopicId());
                }
                break;
            default:
            case RESERVED:
                break;
        }

        if(topicPath == null) {
            logger.log(Level.WARNING, String.format("unable to find matching topicPath in system for [%s] -> [%s]", topicInfo, context));
            throw new MqttsnExpectationFailedException("unable to find matching topicPath in system");
        }
        return topicPath;
    }

    @Override
    public String lookupRegistered(IMqttsnContext context, int topicAlias) throws MqttsnException {
        Map<String, Integer> map = getLookup(context);
        Iterator<String> itr = map.keySet().iterator();
        synchronized (context){
            while(itr.hasNext()){
                String topicPath = itr.next();
                Integer i = map.get(topicPath);
                if(i != null && i.intValue() == topicAlias)
                    return topicPath;
            }
        }
        return null;
    }

    @Override
    public Integer lookupRegistered(IMqttsnContext context, String topicPath) throws MqttsnException {
        Integer alias = null;
        if(topicLookups.containsKey(context)){
            Map<String, Integer> map = getLookup(context);
            alias = map.get(topicPath);
        }
        return alias;
    }

    protected Map<String, Integer> getLookup(IMqttsnContext context){
        Map<String, Integer> map = topicLookups.get(context);
        if(map == null){
            synchronized (this){
                if((map = topicLookups.get(context)) == null){
                    map = createTopicLookup(context);
                    topicLookups.put(context, map);
                }
            }
        }
        return map;
    }

    @Override
    public Integer lookupPredefined(String topicPath) throws MqttsnException {
        Map<String, Integer> predefinedMap = registry.getOptions().getPredefinedTopics();
        return predefinedMap.get(topicPath);
    }

    @Override
    public String lookupPredefined(int topicAlias) throws MqttsnException {
        Map<String, Integer> predefinedMap = registry.getOptions().getPredefinedTopics();
        Iterator<String> itr = predefinedMap.keySet().iterator();
        synchronized (predefinedMap){
            while(itr.hasNext()){
                String topicPath = itr.next();
                Integer i = predefinedMap.get(topicPath);
                if(i != null && i.intValue() == topicAlias)
                    return topicPath;
            }
        }
        return null;
    }

    @Override
    public TopicInfo lookup(IMqttsnContext context, String topicPath) throws MqttsnException {

        //-- check normal first
        TopicInfo info = null;
        if(registered(context, topicPath)){
            Integer topicAlias = lookupRegistered(context, topicPath);
            if(topicAlias != null){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, topicAlias);
            }
        }

        //-- check predefined if nothing in session registry
        if(info == null){
            Integer topicAlias = lookupPredefined(topicPath);
            if(topicAlias != null){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.PREDEFINED, topicAlias);
            }
        }

        //-- if topicPath < 2 chars
        if(info == null){
            if(topicPath.length() <= 2){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.SHORT, topicPath);
            }
        }

        logger.log(Level.INFO, String.format("topic-registry lookup for [%s] => [%s] found [%s]", context, topicPath, info));
        return info;
    }

    @Override
    public TopicInfo normalize(byte topicIdType, byte[] topicData, boolean normalAsLong) throws MqttsnException {
        TopicInfo info = null;
        switch (topicIdType){
            case MqttsnConstants.TOPIC_SHORT:
                if(topicData.length != 2){
                    throw new MqttsnExpectationFailedException("short topics must be exactly 2 bytes");
                }
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.SHORT, new String(topicData, MqttsnConstants.CHARSET));
                break;
            case MqttsnConstants.TOPIC_NORMAL:
                if(normalAsLong){ //-- in the case of a subscribe, the normal actually means the full topic name
                    info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, new String(topicData, MqttsnConstants.CHARSET));
                } else {
                    if(topicData.length != 2){
                        throw new MqttsnExpectationFailedException("normal topic aliases must be exactly 2 bytes");
                    }
                    info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, MqttsnWireUtils.read16bit(topicData[0],topicData[1]));
                }
                break;
            case MqttsnConstants.TOPIC_PREDEFINED:
                if(topicData.length != 2){
                    throw new MqttsnExpectationFailedException("predefined topic aliases must be exactly 2 bytes");
                }
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.PREDEFINED, MqttsnWireUtils.read16bit(topicData[0],topicData[1]));
                break;
        }
        return info;
    }

    protected Map<String, Integer> createTopicLookup(IMqttsnContext context){
        return new HashMap<>();
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
