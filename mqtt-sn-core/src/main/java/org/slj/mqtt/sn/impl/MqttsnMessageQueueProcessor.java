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

import java.util.logging.Level;
import java.util.logging.Logger;

public class MqttsnMessageQueueProcessor<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T> implements IMqttsnMessageQueueProcessor<T>{

    static Logger logger = Logger.getLogger(MqttsnMessageQueueProcessor.class.getName());

    protected boolean clientMode;

    public MqttsnMessageQueueProcessor(boolean clientMode) {
        this.clientMode = clientMode;
    }

    public RESULT process(IMqttsnContext context) throws MqttsnException {

        IMqttsnQueueProcessorStateService stateCheckService = getRegistry().getQueueProcessorStateCheckService();

        synchronized (context){
            //-- if the queue is empty, then something will happen to retrigger this process, ie. message in or out
            //-- so safe to remove
            int count = registry.getMessageQueue().size(context);

            if(logger.isLoggable(Level.FINE)){
                logger.log(Level.FINE,
                        String.format("processing queue size [%s] on thread [%s] for [%s]", count, Thread.currentThread().getName(), context));
            }

            if(count == 0){
                if(stateCheckService != null){
                    //-- this checks on the state of any session and if its AWAKE will lead to a PINGRESP being sent
                    stateCheckService.queueEmpty(context);
                }
                return clientMode ? RESULT.BACKOFF_PROCESS : RESULT.REMOVE_PROCESS;
            }

            //-- this call checks session state to ensure the client has an active or awake session
            if(stateCheckService != null && !stateCheckService.canReceive(context)) {
                return RESULT.REMOVE_PROCESS;
            }

            //-- this checks the inflight if its > 0 we cannot send
            if(!registry.getMessageStateService().canSend(context)) {
                return RESULT.BACKOFF_PROCESS;
            }

            QueuedPublishMessage queuedMessage = registry.getMessageQueue().peek(context);
            String topicPath = queuedMessage.getTopicPath();
            if(queuedMessage != null){
                TopicInfo info = registry.getTopicRegistry().lookup(context, topicPath);
                if(info == null){
                    logger.log(Level.INFO, String.format("need to register for delivery to [%s] on topic [%s]", context, topicPath));
                    if(!clientMode){
                        //-- only the server hands out alias's
                        info = registry.getTopicRegistry().register(context, topicPath);
                    }
                    IMqttsnMessage register = registry.getMessageFactory().createRegister(info != null ? info.getTopicId() : 0, topicPath);
                    try {
                        MqttsnWaitToken token = registry.getMessageStateService().sendMessage(context, register);
                        if(clientMode){
                            if(token != null){
                                registry.getMessageStateService().waitForCompletion(context, token);
                            }
                        }
                    } catch(MqttsnExpectationFailedException e){
                        logger.log(Level.WARNING, String.format("unable to send message, try again later"), e);
                    }
                    //-- with a register we should come back when the registration is complete and attempt delivery
                    return RESULT.BACKOFF_PROCESS;
                } else {
                    //-- only deque when we have confirmed we can deliver
                    queuedMessage = registry.getMessageQueue().pop(context);
                    if (queuedMessage != null) {
                        queuedMessage.incrementRetry();
                        //-- let the reaper check on delivery
                        try {
                            MqttsnWaitToken token = registry.getMessageStateService().sendMessage(context, info, queuedMessage);
                            if (clientMode) {
                                if(token != null){
                                    registry.getMessageStateService().waitForCompletion(context, token);
                                }
                            }

                        } catch (MqttsnException e) {
                            logger.log(Level.WARNING, String.format("unable to send message, requeue and backoff"), e);
                            registry.getMessageQueue().offer(context, queuedMessage);
                        }
                    }

                    return RESULT.REPROCESS;
                }
            } else {
                return clientMode ? RESULT.BACKOFF_PROCESS : RESULT.REMOVE_PROCESS;
            }
        }
    }
}
