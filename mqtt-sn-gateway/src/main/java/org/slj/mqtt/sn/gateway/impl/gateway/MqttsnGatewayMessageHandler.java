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
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.gateway.spi.*;
import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.gateway.spi.gateway.MqttsnGatewayOptions;
import org.slj.mqtt.sn.impl.AbstractMqttsnMessageHandler;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.IMqttsnSessionState;
import org.slj.mqtt.sn.model.MqttsnClientState;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.wire.version1_2.payload.*;

import java.util.Set;
import java.util.logging.Level;

public class MqttsnGatewayMessageHandler
        extends AbstractMqttsnMessageHandler<IMqttsnGatewayRuntimeRegistry> {

    protected IMqttsnSessionState getSessionState(IMqttsnContext context) throws MqttsnException, MqttsnInvalidSessionStateException {
        IMqttsnSessionState state = registry.getGatewaySessionService().getSessionState(context, false);
        if(state == null || state.getClientState() == MqttsnClientState.DISCONNECTED)
            throw new MqttsnInvalidSessionStateException("session not available for context");
        return state;
    }

    protected IMqttsnSessionState getSessionState(IMqttsnContext context, boolean createIfNotExists) throws MqttsnException {
        IMqttsnSessionState state = registry.getGatewaySessionService().getSessionState(context, createIfNotExists);
        return state;
    }

    @Override
    protected void beforeHandle(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {

        try {
            //see if the client has an active session
            getSessionState(context);
        } catch(MqttsnInvalidSessionStateException e){

            //if they do NOT, the only time we can process messages
            //on their behalf is if its a CONNECT or a PUBLISH M 1
            boolean shouldContinue = false;
            if(message instanceof MqttsnConnect){
                //this is ok
                shouldContinue = true;
            } else if(message instanceof MqttsnPublish){
                MqttsnPublish p = (MqttsnPublish) message;
                if(p.getQoS() == MqttsnConstants.QoSM1){
                    //this is ok
                    shouldContinue = true;
                }
            }
            if(!shouldContinue){
                logger.log(Level.WARNING, String.format("detected invalid client session state for [%s] and inbound message [%s]", context, message));
                throw new MqttsnException(e);
            }
        }
    }

    @Override
    protected void afterHandle(IMqttsnContext context, IMqttsnMessage messageIn, IMqttsnMessage messageOut) throws MqttsnException {

        try {
            IMqttsnSessionState sessionState = getSessionState(context);
            if(sessionState != null){
                registry.getGatewaySessionService().updateLastSeen(sessionState);
            }
        } catch(MqttsnInvalidSessionStateException e){
            //-- a disconnect will mean theres no session to update
        }
    }

    @Override
    protected IMqttsnMessage handleConnect(IMqttsnContext context, IMqttsnMessage connect) throws MqttsnException, MqttsnCodecException {

        MqttsnConnect connectMessage = (MqttsnConnect) connect ;
        if(!validateClientId(context, connectMessage.getClientId())){

            logger.log(Level.WARNING, String.format("rejected client based on non-listed clientId [%s]", connectMessage.getClientId()));
            return registry.getMessageFactory().createConnack(MqttsnConstants.RETURN_CODE_SERVER_UNAVAILABLE);

        } else {
            IMqttsnSessionState state = getSessionState(context, true);
            ConnectResult result = registry.getGatewaySessionService().connect(state, connectMessage.getClientId(),
                    connectMessage.getDuration(), connectMessage.isCleanSession());

            processSessionResult(result);
            if(result.isError()){
                return registry.getMessageFactory().createConnack(result.getReturnCode());
            }
            else {
                if(connectMessage.isWill()){
                    return registry.getMessageFactory().createWillTopicReq();
                } else {
                    return registry.getMessageFactory().createConnack(result.getReturnCode());
                }
            }
        }
    }

    protected boolean validateClientId(IMqttsnContext context, String clientId){
        Set<String> allowedClientId = ((MqttsnGatewayOptions)registry.getOptions()).getAllowedClientIds();
        if(allowedClientId != null && !allowedClientId.isEmpty()){
            return allowedClientId.contains(clientId);
        }
        return true;
    }

    @Override
    protected IMqttsnMessage handleDisconnect(IMqttsnContext context, IMqttsnMessage initialDisconnect, IMqttsnMessage receivedDisconnect) throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        IMqttsnSessionState state = getSessionState(context);
        MqttsnDisconnect d = (MqttsnDisconnect) receivedDisconnect;
        registry.getGatewaySessionService().disconnect(state, d.getDuration());
        return super.handleDisconnect(context, initialDisconnect, receivedDisconnect);
    }

    @Override
    protected IMqttsnMessage handlePingreq(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        IMqttsnSessionState state = getSessionState(context);
        registry.getGatewaySessionService().ping(state);
        return super.handlePingreq(context, message);
    }

    @Override
    protected IMqttsnMessage handleSubscribe(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        MqttsnSubscribe subscribe = (MqttsnSubscribe) message;
        IMqttsnSessionState state = getSessionState(context);
        TopicInfo info = registry.getTopicRegistry().normalize((byte) subscribe.getTopicType(), subscribe.getTopicData(), true);
        SubscribeResult result = registry.getGatewaySessionService().subscribe(state, info, subscribe.getQoS());
        processSessionResult(result);
        return registry.getMessageFactory().createSuback(result.getGrantedQoS(), result.getTopicInfo().getTopicId(), result.getReturnCode());
    }

    @Override
    protected IMqttsnMessage handleUnsubscribe(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        MqttsnUnsubscribe unsubscribe = (MqttsnUnsubscribe) message;
        IMqttsnSessionState state = getSessionState(context);
        TopicInfo info = registry.getTopicRegistry().normalize((byte) unsubscribe.getTopicType(), unsubscribe.getTopicData(), true);
        UnsubscribeResult result = registry.getGatewaySessionService().unsubscribe(state, info);
        processSessionResult(result);
        return registry.getMessageFactory().createUnsuback();
    }

    @Override
    protected IMqttsnMessage handleRegister(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        MqttsnRegister register = (MqttsnRegister) message;
        IMqttsnSessionState state = getSessionState(context);
        RegisterResult result = registry.getGatewaySessionService().register(state, register.getTopicName());
        processSessionResult(result);
        return registry.getMessageFactory().createRegack(result.getTopicInfo().getTopicId(), MqttsnConstants.RETURN_CODE_ACCEPTED);
    }

    @Override
    protected IMqttsnMessage handlePublish(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException, MqttsnInvalidSessionStateException {

        MqttsnPublish publish = (MqttsnPublish) message;
        IMqttsnSessionState state = null;
        try {
            state = getSessionState(context);
        } catch(MqttsnInvalidSessionStateException e){
            //-- connectionless publish (m1)
            if(publish.getQoS() >= 0)
                throw e;
        }

        TopicInfo info = registry.getTopicRegistry().normalize((byte) publish.getTopicType(), publish.getTopicData(), false);
        PublishResult result = registry.getGatewaySessionService().publish(state, info, Math.min(2, Math.max(0, publish.getQoS())), publish.getData());
        processSessionResult(result);

        IMqttsnMessage response = null;
        if(result.getStatus() == Result.STATUS.ERROR){
            //-- if there is an error on publish any message may return a Puback with error
            response = registry.getMessageFactory().createPuback(publish.readTopicDataAsInteger(),
                    result.getReturnCode());
        } else {
            switch (publish.getQoS()) {
                case MqttsnConstants.QoS1:
                    response = registry.getMessageFactory().createPuback(publish.readTopicDataAsInteger(),
                            result.getReturnCode());
                    break;
                case MqttsnConstants.QoS2:
                    response = registry.getMessageFactory().createPubrec();
                    break;

                default:
                case MqttsnConstants.QoSM1:
                case MqttsnConstants.QoS0:
                    break;
            }
        }
        return response;
    }

    protected void processSessionResult(Result result){
        if(result.getStatus() != Result.STATUS.SUCCESS){
            logger.log(Level.WARNING, String.format("error detected by session service [%s]", result));
        }
    }
}