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

package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.MqttsnWaitToken;
import org.slj.mqtt.sn.model.QueuedPublishMessage;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MqttsnMessageQueueProcessor<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T> implements IMqttsnMessageQueueProcessor<T>{

    static Logger logger = Logger.getLogger(MqttsnMessageQueueProcessor.class.getName());

    protected boolean clientMode;

    public MqttsnMessageQueueProcessor(boolean clientMode){
        this.clientMode = clientMode;
    }

    public void process(IMqttsnContext context) throws MqttsnException {
        if(registry.getMessageStateService().countInflight(context) == 0){//double check lock after sync
            if(registry.getMessageQueue().size(context) > 0){
                QueuedPublishMessage message = registry.getMessageQueue().peek(context);
                String topicPath = message.getTopicPath();
                if(message != null){
                    TopicInfo info = registry.getTopicRegistry().lookup(context, topicPath);
                    if(info == null){
                        logger.log(Level.INFO, String.format("need to register for delivery to [%s] on topic [%s]", context, topicPath));
                        if(!clientMode){
                            //-- only the server hands out alias's
                            info = registry.getTopicRegistry().register(context, topicPath);
                        }
                        IMqttsnMessage register = registry.getMessageFactory().createRegister(info != null ? info.getTopicId() : 0, topicPath);
                        registry.getMessageStateService().sendMessage(context, register);
                    } else {
                        //-- only deque when we have confirmed we can deliver
                        message = registry.getMessageQueue().pop(context);
                        message.incrementRetry();
                        IMqttsnMessage publish = registry.getMessageFactory().createPublish(message.getGrantedQoS(),
                                message.getRetryCount() > 1, false, info.getType(), info.getTopicId(), message.getPayload());
                        logger.log(Level.INFO, String.format("sending queued message to [%s] on topic [%s]", context, topicPath));

                        //-- let the reaper check on delivery
                        try {
                            registry.getMessageStateService().sendMessage(context, publish, message);
                        } catch(MqttsnException e){
                            logger.log(Level.WARNING, String.format("unable to send message having checked inflight, detected collision, requeue and backoff"));
                            registry.getMessageQueue().offer(context, message);
                        }
                    }
                }
            }
        }
    }
}
