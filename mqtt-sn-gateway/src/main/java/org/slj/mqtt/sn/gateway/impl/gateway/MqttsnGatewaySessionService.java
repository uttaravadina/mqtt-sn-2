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

package org.slj.mqtt.sn.gateway.impl.gateway;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.gateway.spi.*;
import org.slj.mqtt.sn.gateway.spi.broker.MqttsnBrokerException;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewaySessionRegistryService;
import org.slj.mqtt.sn.gateway.spi.gateway.MqttsnGatewayOptions;
import org.slj.mqtt.sn.impl.AbstractMqttsnBackoffThreadService;
import org.slj.mqtt.sn.impl.MqttsnMessageQueueProcessor;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.utils.TopicPath;

import java.util.*;
import java.util.logging.Level;

public class MqttsnGatewaySessionService extends AbstractMqttsnBackoffThreadService<IMqttsnGatewayRuntimeRegistry>
        implements IMqttsnGatewaySessionRegistryService {

    protected Map<IMqttsnContext, IMqttsnSessionState> sessionLookup;

    @Override
    public void start(IMqttsnGatewayRuntimeRegistry runtime) throws MqttsnException {
        super.start(runtime);
        sessionLookup = Collections.synchronizedMap(new HashMap());
    }

    @Override
    protected boolean doWork() {
        Iterator<IMqttsnContext> itr = sessionLookup.keySet().iterator();
        synchronized (sessionLookup){
            while(itr.hasNext()){
                IMqttsnContext context = itr.next();
                IMqttsnSessionState state = sessionLookup.get(context);
                deamon_validateKeepAlive(state);
            }
        }
        return true;
    }

    protected void deamon_validateKeepAlive(IMqttsnSessionState state){
        if(state.getClientState() == MqttsnClientState.CONNECTED ||
                state.getClientState() == MqttsnClientState.ASLEEP){
            long time = System.currentTimeMillis();
            if(state != null && state.getKeepAlive() > 0){
                long lastSeen = state.getLastSeen().getTime();
                if(lastSeen + (state.getKeepAlive() * 1000) < time){
                    logger.log(Level.WARNING, String.format("keep-alive deamon detected stale session for [%s], disconnecting", state.getContext()));
                    state.setClientState(MqttsnClientState.DISCONNECTED);
                }
            }
        }
    }

    @Override
    public IMqttsnSessionState getSessionState(IMqttsnContext context, boolean createIfNotExists) {
        IMqttsnSessionState state = sessionLookup.get(context);
        if(state == null && createIfNotExists){
            synchronized (this){
                if((state = sessionLookup.get(context)) == null){
                    state = new MqttsnSessionState(context, MqttsnClientState.PENDING);
                    sessionLookup.put(context, state);
                }
            }
        }
        return state;
    }

    @Override
    public ConnectResult connect(IMqttsnSessionState state, String clientId, int keepAlive, boolean cleanSession) throws MqttsnException {
        ConnectResult result = null;
        result = processAllowList(clientId);
        if(result == null){
            synchronized (state.getContext()){
                try {
                    result = registry.getBrokerService().connect(state.getContext(), state.getContext().getId(), cleanSession, keepAlive);
                } finally {
                    if(!result.isError()){
                        //clear down all prior session state
                        cleanSession(state.getContext(), cleanSession);
                        state.setKeepAlive(keepAlive);
                        state.setClientState(MqttsnClientState.CONNECTED);
                    } else {
                        //-- connect was not successful ensure we
                        //-- do not hold a reference to any session
                        clear(state.getContext());
                    }
                }
            }
        }

        logger.log(Level.INFO, String.format("handled connection request for [%s] with cleanSession [%s] -> [%s], [%s]", state.getContext(), cleanSession, result.getStatus(), result.getMessage()));
        return result;
    }

    @Override
    public void disconnect(IMqttsnSessionState state, int duration) throws MqttsnException {
        DisconnectResult result = null;
        synchronized (state.getContext()){
            result = registry.getBrokerService().disconnect(state.getContext(), duration);
            if(!result.isError()){
                if(duration > 0){
                    logger.log(Level.INFO, String.format("[%s] setting client state asleep for [%s]", state.getContext(), duration));
                    state.setKeepAlive(duration);
                    state.setClientState(MqttsnClientState.ASLEEP);
                } else {
                    logger.log(Level.INFO, String.format("[%s] disconnecting client", state.getContext()));
                    sessionLookup.remove(state.getContext());
                }
            }
        }
    }

    @Override
    public SubscribeResult subscribe(IMqttsnSessionState state, TopicInfo info, int QoS) throws MqttsnException {

        IMqttsnContext context = state.getContext();
        synchronized (context){
            String topicPath = null;
            if(info.getType() == MqttsnConstants.TOPIC_TYPE.PREDEFINED){
                topicPath = registry.getTopicRegistry().lookupPredefined(context, info.getTopicId());
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.PREDEFINED, info.getTopicId());
            } else {
                topicPath = info.getTopicPath();
                if(!TopicPath.isValidSubscription(topicPath, registry.getOptions().getMaxTopicLength())){
                    return new SubscribeResult(Result.STATUS.ERROR, MqttsnConstants.RETURN_CODE_INVALID_TOPIC_ID,
                            "invalid topic format");
                }
                if(!TopicPath.isWild(topicPath)){
                    info = registry.getTopicRegistry().lookup(state.getContext(), topicPath);
                    if(info == null){
                        info = registry.getTopicRegistry().register(state.getContext(), topicPath);
                    }
                } else {
                    info = TopicInfo.WILD;
                }
            }

            if(registry.getSubscriptionRegistry().subscribe(state.getContext(), topicPath, QoS)){
                SubscribeResult result = registry.getBrokerService().subscribe(context, topicPath, QoS);
                result.setTopicInfo(info);
                return result;
            } else {
                SubscribeResult result = new SubscribeResult(Result.STATUS.NOOP);
                result.setTopicInfo(info);
                result.setGrantedQoS(QoS);
                return result;
            }
        }
    }

    @Override
    public UnsubscribeResult unsubscribe(IMqttsnSessionState state, TopicInfo info) throws MqttsnException {
        if(!TopicPath.isValidSubscription(info.getTopicPath(), registry.getOptions().getMaxTopicLength())){
            return new UnsubscribeResult(Result.STATUS.ERROR, MqttsnConstants.RETURN_CODE_INVALID_TOPIC_ID,
                    "invalid topic format");
        }
        IMqttsnContext context = state.getContext();
        synchronized (context){
            String topicPath = info.getTopicPath();
            if(registry.getSubscriptionRegistry().unsubscribe(context, topicPath)){
                UnsubscribeResult result = registry.getBrokerService().unsubscribe(context, topicPath);
                return result;
            } else {
                return new UnsubscribeResult(Result.STATUS.NOOP);
            }
        }
    }

    @Override
    public RegisterResult register(IMqttsnSessionState state, String topicPath) throws MqttsnException {

        if(!TopicPath.isValidSubscription(topicPath, registry.getOptions().getMaxTopicLength())){
            return new RegisterResult(Result.STATUS.ERROR, MqttsnConstants.RETURN_CODE_INVALID_TOPIC_ID, "invalid topic format");
        }
        synchronized (state.getContext()){
            TopicInfo info;
            if(!TopicPath.isWild(topicPath)){
                info = registry.getTopicRegistry().lookup(state.getContext(), topicPath);
                if(info == null){
                    info = registry.getTopicRegistry().register(state.getContext(), topicPath);
                }
            } else {
                info = TopicInfo.WILD;
            }
            return new RegisterResult(topicPath, info);
        }
    }

    @Override
    public void ping(IMqttsnSessionState state) {
        updateLastSeen(state);
    }

    @Override
    public void updateLastSeen(IMqttsnSessionState state) {
        state.setLastSeen(new Date());
    }

    public void cleanSession(IMqttsnContext context, boolean deepClean) throws MqttsnException {

        //clear down all prior session state
        synchronized (context){
            if(deepClean){
                //-- the queued messages
                registry.getMessageQueue().clear(context);

                //-- the subscriptions
                registry.getSubscriptionRegistry().clear(context);
            }

            //-- inflight messages & protocol messages
            registry.getMessageStateService().clear(context);

            //-- topic registrations
            registry.getTopicRegistry().clear(context);
        }
    }

    public void clearAll() {
        sessionLookup.clear();
    }

    @Override
    public void clear(IMqttsnContext context) {
        logger.log(Level.INFO, String.format(String.format("removing context from active/sleepings sessions [%s]", context)));
        sessionLookup.remove(context);
    }

    protected ConnectResult processAllowList(String clientId){
        Set<String> allowedClientIds = ((MqttsnGatewayOptions) registry.getOptions()).getAllowedClientIds();
        if(allowedClientIds != null && !allowedClientIds.isEmpty()){
            if(!allowedClientIds.contains(clientId)){
                return new ConnectResult(Result.STATUS.ERROR, MqttsnConstants.RETURN_CODE_SERVER_UNAVAILABLE, "client id not allowed");
            }
        }

        int maxConnectedClients = ((MqttsnGatewayOptions) registry.getOptions()).getMaxConnectedClients();
        if(sessionLookup.size() >= maxConnectedClients){
            return new ConnectResult(Result.STATUS.ERROR, MqttsnConstants.RETURN_CODE_REJECTED_CONGESTION, "gateway has reached capacity");
        }
        return null;
    }

    @Override
    public void receiveToSessions(String topicPath, byte[] payload, int QoS) throws MqttsnException {
        //-- expand the message onto the gateway connected device queues
        List<IMqttsnContext> recipients = registry.getSubscriptionRegistry().matches(topicPath);
        logger.log(Level.INFO, String.format("receiving broker side message into [%s] sessions", recipients.size()));

        //if we only have 1 reciever remove message after read
        UUID messageId = recipients.size() > 1 ?
                registry.getMessageRegistry().add(payload, calculateExpiry()) :
                registry.getMessageRegistry().add(payload, true) ;

        for (IMqttsnContext client : recipients){
            int grantedQos = registry.getSubscriptionRegistry().getQos(client, topicPath);
            int q = Math.min(grantedQos,QoS);
            registry.getMessageQueue().offer(client, new QueuedPublishMessage(
                    messageId, topicPath, q));
        }
    }

    protected Date calculateExpiry(){
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.YEAR, 1);
        return cal.getTime();
    }
}
