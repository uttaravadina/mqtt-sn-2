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

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.model.InflightMessage;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.*;

import java.util.logging.Level;

public abstract class AbstractMqttsnMessageHandler<U extends IMqttsnRuntimeRegistry>
        extends MqttsnService<U> implements IMqttsnMessageHandler {

    public boolean authorizeContext(INetworkContext context, String clientId) {
        try {
            IMqttsnContext mqttsnContext = registry.getContextFactory().createInitialContext(context, clientId);
            if(mqttsnContext != null){
                context.setMqttsnContext(mqttsnContext);
                registry.getNetworkRegistry().putContext(context);
                return true;
            }
            logger.log(Level.WARNING, String.format("context factory did not provide secured context, refuse auth"));
            return false;
        }
        catch(NetworkRegistryException e){
            logger.log(Level.WARNING, String.format("error interacting with network registry, refuse auth"), e);
            return false;
        }
        catch(MqttsnSecurityException e){
            logger.log(Level.WARNING, String.format("security exception detected, refuse auth"), e);
            return false;
        }
    }

    @Override
    public boolean validResponse(IMqttsnMessage message, Class<? extends IMqttsnMessage> cls) {
        Class<? extends IMqttsnMessage>[] clz = getResponseClasses(message);
        return MqttsnUtils.contains(clz, cls);
    }

    protected Class<? extends IMqttsnMessage>[] getResponseClasses(IMqttsnMessage message) {

        if(!requiresResponse(message)){
            return new Class[0];
        }
        switch(message.getMessageType()){
            case MqttsnConstants.CONNECT:
                return new Class[]{ MqttsnConnack.class };
            case MqttsnConstants.PUBLISH:
                return new Class[]{ MqttsnPuback.class, MqttsnPubrec.class, MqttsnPubrel.class, MqttsnPubcomp.class };
            case MqttsnConstants.PUBREC:
                return new Class[]{ MqttsnPubrel.class };
            case MqttsnConstants.PUBREL:
                return new Class[]{ MqttsnPubcomp.class };
            case MqttsnConstants.SUBSCRIBE:
                return new Class[]{ MqttsnSuback.class };
            case MqttsnConstants.UNSUBSCRIBE:
                return new Class[]{ MqttsnUnsuback.class };
            case MqttsnConstants.REGISTER:
                return new Class[]{ MqttsnRegack.class };
            case MqttsnConstants.PINGREQ:
                return new Class[]{ MqttsnPingresp.class };
            case MqttsnConstants.DISCONNECT:
                return new Class[]{ MqttsnDisconnect.class };
            case MqttsnConstants.SEARCHGW:
                return new Class[]{ MqttsnGwInfo.class };
            case MqttsnConstants.WILLMSGREQ:
                return new Class[]{ MqttsnWillmsg.class };
            case MqttsnConstants.WILLTOPICREQ:
                return new Class[]{ MqttsnWilltopic.class };
            case MqttsnConstants.WILLTOPICUPD:
                return new Class[]{ MqttsnWilltopicresp.class };
            case MqttsnConstants.WILLMSGUPD:
                return new Class[]{ MqttsnWillmsgresp.class };
            default:
                throw new MqttsnRuntimeException(
                        String.format("invalid message type detected [%s], non terminal and non response!", message.getMessageName()));
        }
    }

    @Override
    public boolean isTerminalMessage(IMqttsnMessage message) {
        switch(message.getMessageType()){
            case MqttsnConstants.PUBLISH:
                MqttsnPublish publish = (MqttsnPublish) message;
                return publish.getQoS() <= 0;
            case MqttsnConstants.CONNACK:
            case MqttsnConstants.PUBACK:    //we delete QoS 1 sent PUBLISH on receipt of PUBACK
            case MqttsnConstants.PUBREL:    //we delete QoS 2 sent PUBLISH on receipt of PUBREL
            case MqttsnConstants.UNSUBACK:
            case MqttsnConstants.SUBACK:
            case MqttsnConstants.ADVERTISE:
            case MqttsnConstants.REGACK:
            case MqttsnConstants.PUBCOMP:   //we delete QoS 2 received PUBLISH on receipt of PUBCOMP
            case MqttsnConstants.PINGRESP:
            case MqttsnConstants.DISCONNECT:
            case MqttsnConstants.ENCAPSMSG:
            case MqttsnConstants.GWINFO:
            case MqttsnConstants.WILLMSG:
            case MqttsnConstants.WILLMSGRESP:
            case MqttsnConstants.WILLTOPIC:
            case MqttsnConstants.WILLTOPICRESP:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean requiresResponse(IMqttsnMessage message) {
        switch(message.getMessageType()){
            case MqttsnConstants.PUBLISH:
                    MqttsnPublish publish = (MqttsnPublish) message;
                    return publish.getQoS() > 0;
            case MqttsnConstants.CONNECT:
            case MqttsnConstants.PUBREC:
            case MqttsnConstants.PUBREL:
            case MqttsnConstants.SUBSCRIBE:
            case MqttsnConstants.UNSUBSCRIBE:
            case MqttsnConstants.REGISTER:
            case MqttsnConstants.PINGREQ:
            case MqttsnConstants.DISCONNECT:
            case MqttsnConstants.SEARCHGW:
            case MqttsnConstants.WILLMSGREQ:
            case MqttsnConstants.WILLMSGUPD:
            case MqttsnConstants.WILLTOPICREQ:
            case MqttsnConstants.WILLTOPICUPD:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isPartOfOriginatingMessage(IMqttsnMessage message) {
        switch(message.getMessageType()){
            case MqttsnConstants.PUBLISH:
            case MqttsnConstants.CONNECT:
            case MqttsnConstants.SUBSCRIBE:
            case MqttsnConstants.UNSUBSCRIBE:
            case MqttsnConstants.REGISTER:
            case MqttsnConstants.PINGREQ:
            case MqttsnConstants.DISCONNECT:
            case MqttsnConstants.SEARCHGW:
            case MqttsnConstants.WILLMSGREQ:
            case MqttsnConstants.WILLMSGUPD:
            case MqttsnConstants.WILLTOPICREQ:
            case MqttsnConstants.WILLTOPICUPD:
                return true;
            default:
                return false;
        }
    }

    @Override
    public void receiveMessage(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException {

        try {

            int msgType = message.getMessageType();

            if(message.isErrorMessage()){
                logger.log(Level.WARNING, String.format("mqtt-sn handler [%s] received error message [%s]",
                        context, message));
            }

            beforeHandle(context, message);

            IMqttsnMessage originatingMessage = null;

            logger.log(Level.INFO, String.format("mqtt-sn handler [%s] handling inbound message [%s]",
                    context, message));

            boolean errord = false;
            if(registry.getMessageStateService() != null){
                try {
                    originatingMessage =
                            registry.getMessageStateService().notifyMessageReceived(context, message);
                } catch(MqttsnException e){
                    errord = true;
                    logger.log(Level.WARNING, String.format("mqtt-sn state service errord, allow message lifecycle to handle [%s] -> [%s]",
                            context, e.getMessage()));
                }
            }

            IMqttsnMessage response = null;
            switch (msgType) {
                case MqttsnConstants.CONNECT:
                    response = handleConnect(context, message);
                    break;
                case MqttsnConstants.CONNACK:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handleConnack(context, originatingMessage, message);
                    break;
                case MqttsnConstants.PUBLISH:
                    if(errord){
                        response = getRegistry().getMessageFactory().createPuback(0,
                                MqttsnConstants.RETURN_CODE_SERVER_UNAVAILABLE);
                    } else {
                        response = handlePublish(context, message);
                    }
                    break;
                case MqttsnConstants.PUBREC:
                    response = handlePubrec(context, message);
                    break;
                case MqttsnConstants.PUBREL:
                    response = handlePubrel(context, message);
                    break;
                case MqttsnConstants.PUBACK:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handlePuback(context, originatingMessage, message);
                    break;
                case MqttsnConstants.PUBCOMP:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handlePubcomp(context, originatingMessage, message);
                    break;
                case MqttsnConstants.SUBSCRIBE:
                    if(errord){
                        response = getRegistry().getMessageFactory().createSuback(0, 0,
                                MqttsnConstants.RETURN_CODE_SERVER_UNAVAILABLE);
                    } else {
                        response = handleSubscribe(context, message);
                    }
                    break;
                case MqttsnConstants.UNSUBSCRIBE:
                    response = handleUnsubscribe(context, message);
                    break;
                case MqttsnConstants.UNSUBACK:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handleUnsuback(context, originatingMessage, message);
                    break;
                case MqttsnConstants.SUBACK:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handleSuback(context, originatingMessage, message);
                    break;
                case MqttsnConstants.REGISTER:
                    if(errord){
                        response = getRegistry().getMessageFactory().createRegack(0,
                                MqttsnConstants.RETURN_CODE_SERVER_UNAVAILABLE);
                    } else {
                        response = handleRegister(context, message);
                    }
                    break;
                case MqttsnConstants.REGACK:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handleRegack(context, originatingMessage, message);
                    break;
                case MqttsnConstants.PINGREQ:
                    response = handlePingreq(context, message);
                    break;
                case MqttsnConstants.PINGRESP:
                    validateOriginatingMessage(context, originatingMessage, message);
                    handlePingresp(context, originatingMessage, message);
                    break;
                case MqttsnConstants.DISCONNECT:
                    response = handleDisconnect(context, originatingMessage, message);
                    break;
                case MqttsnConstants.ADVERTISE:
                    handleAdvertise(context, message);
                    break;
                case MqttsnConstants.ENCAPSMSG:
                    handleEncapsmsg(context, message);
                    break;
                case MqttsnConstants.GWINFO:
                    handleGwinfo(context, message);
                    break;
                case MqttsnConstants.SEARCHGW:
                    response = handleSearchGw(context, message);
                    break;
                case MqttsnConstants.WILLMSGREQ:
                    response = handleWillmsgreq(context, message);
                    break;
                case MqttsnConstants.WILLMSG:
                    handleWillmsg(context, message);
                    break;
                case MqttsnConstants.WILLMSGUPD:
                    response = handleWillmsgupd(context, message);
                    break;
                case MqttsnConstants.WILLMSGRESP:
                    handleWillmsgresp(context, message);
                    break;
                case MqttsnConstants.WILLTOPICREQ:
                    response = handleWilltopicreq(context, message);
                    break;
                case MqttsnConstants.WILLTOPIC:
                    handleWilltopic(context, message);
                    break;
                case MqttsnConstants.WILLTOPICUPD:
                    response = handleWilltopicupd(context, message);
                    break;
                case MqttsnConstants.WILLTOPICRESP:
                    handleWilltopicresp(context, message);
                    break;
                default:
                    throw new MqttsnException("unable to handle unknown message type " + msgType);
            }
            //-- if the state service threw a wobbler but for some reason this didnt lead to an error message
            //-- we should just disconnect the device
            if(errord && !response.isErrorMessage()){
                logger.log(Level.WARNING, String.format("mqtt-sn state service errord, message handler did not produce an error, so overrule and disoconnect [%s] -> [%s]",
                        context, message));
                response = registry.getMessageFactory().createDisconnect();
            }

            //-- this tidies up inflight if there are errors
            afterHandle(context, message, response);

            if (response != null) {
                if (response.needsMsgId() && response.getMsgId() == 0) {
                    int msgId = message.getMsgId();
                    response.setMsgId(msgId);
                }

                handleResponse(context, response);
            }

            afterResponse(context, message, response);

        } catch(MqttsnException e){
            logger.log(Level.SEVERE, "error encountered during receive, disconnect device", e);
            handleResponse(context,
                    registry.getMessageFactory().createDisconnect());
            throw e;
        }
    }

    protected abstract void beforeHandle(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException;

    protected void afterHandle(IMqttsnContext context, IMqttsnMessage message, IMqttsnMessage response) throws MqttsnException {

        if(response != null && response.isErrorMessage()){
            //we need to remove any message that was marked inflight
            if(message.needsMsgId()){
                if(registry.getMessageStateService().removeInflight(context, message.getMsgId()) != null){
                    logger.log(Level.WARNING, "tidied up bad message that was marked inflight and yeilded error response");
                }
            }
        }
    }

    protected void afterResponse(IMqttsnContext context, IMqttsnMessage message, IMqttsnMessage response) throws MqttsnException {
    }

    protected void validateOriginatingMessage(IMqttsnContext context, IMqttsnMessage originatingMessage, IMqttsnMessage message)
        throws MqttsnExpectationFailedException{
        if(originatingMessage == null){
            logger.log(Level.SEVERE, String.format("[%s] no originating message found for acknowledgement [%s]", context, message));
            throw new MqttsnExpectationFailedException("no originating message found for acknowledgement");
        }
    }

    protected void handleResponse(IMqttsnContext context, IMqttsnMessage response)
            throws MqttsnException {

        logger.log(Level.INFO, String.format("mqtt-sn handler [%s] sending outbound message [%s]",
                context, response));
        registry.getTransport().writeToTransport(context.getNetworkContext(), response);
    }

    protected IMqttsnMessage handleConnect(IMqttsnContext context, IMqttsnMessage connect) throws MqttsnException {

        MqttsnConnect connectMessage = (MqttsnConnect) connect ;
        if(connectMessage.isWill()){
            return registry.getMessageFactory().createWillTopicReq();
        } else {
            return registry.getMessageFactory().createConnack(MqttsnConstants.RETURN_CODE_ACCEPTED);
        }
    }

    protected void handleConnack(IMqttsnContext context, IMqttsnMessage connect, IMqttsnMessage connack) throws MqttsnException {
    }

    protected IMqttsnMessage handleDisconnect(IMqttsnContext context, IMqttsnMessage originatingMessage, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException {

        //-- if the disconnect is received in response to a disconnect we sent, lets not send another!
        if(originatingMessage != null){
            logger.log(Level.INFO, "disconnect received in response to my disconnect, dont send another!");
            return null;
        } else {
            if(registry.getRuntime().disconnectReceived(context)){
                return registry.getMessageFactory().createDisconnect();
            }
            return null;
        }
    }

    protected IMqttsnMessage handlePingreq(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException {
        return registry.getMessageFactory().createPingresp();
    }

    protected void handlePingresp(IMqttsnContext context, IMqttsnMessage originatingMessage, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleSubscribe(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException {

        MqttsnSubscribe subscribe = (MqttsnSubscribe) message;
        return registry.getMessageFactory().createSuback(subscribe.getQoS(), 0x00, MqttsnConstants.RETURN_CODE_ACCEPTED);
    }

    protected IMqttsnMessage handleUnsubscribe(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException, MqttsnCodecException {
        MqttsnUnsubscribe unsubscribe = (MqttsnUnsubscribe) message;
        return registry.getMessageFactory().createUnsuback();
    }

    protected void handleSuback(IMqttsnContext context, IMqttsnMessage initial, IMqttsnMessage message) throws MqttsnException {
        MqttsnSuback suback = (MqttsnSuback) message;
        MqttsnSubscribe subscribe = (MqttsnSubscribe) initial;
        String topicPath = null;
        if(subscribe.getTopicType() == MqttsnConstants.TOPIC_NORMAL){
            topicPath = subscribe.getTopicName();
            registry.getTopicRegistry().register(context, topicPath, suback.getTopicId());
        } else {
            topicPath = registry.getTopicRegistry().topicPath(context,
                    registry.getTopicRegistry().normalize((byte) subscribe.getTopicType(), subscribe.getTopicData(), false), false);
        }

        registry.getSubscriptionRegistry().subscribe(context, topicPath, suback.getQoS());
    }

    protected void handleUnsuback(IMqttsnContext context, IMqttsnMessage unsubscribe, IMqttsnMessage unsuback) throws MqttsnException {
        String topicPath = ((MqttsnUnsubscribe)unsubscribe).getTopicName();
        registry.getSubscriptionRegistry().unsubscribe(context, topicPath);
    }

    protected IMqttsnMessage handleRegister(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException {

        MqttsnRegister register = (MqttsnRegister) message;
        return registry.getMessageFactory().createRegack(register.getTopicId(), MqttsnConstants.RETURN_CODE_ACCEPTED);
    }

    protected void handleRegack(IMqttsnContext context, IMqttsnMessage register, IMqttsnMessage regack) throws MqttsnException {

        String topicPath = ((MqttsnRegister)register).getTopicName();
        registry.getTopicRegistry().register(context, topicPath, ((MqttsnRegack)regack).getTopicId());
    }

    protected IMqttsnMessage handlePublish(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException {

        MqttsnPublish publish = (MqttsnPublish) message;
        IMqttsnMessage response = null;

        TopicInfo info = registry.getTopicRegistry().normalize((byte) publish.getTopicType(), publish.getTopicData(), false);
        String topicPath = registry.getTopicRegistry().topicPath(context, info, true);
        if(registry.getPermissionService() != null){
            if(!registry.getPermissionService().allowedToPublish(context, topicPath, publish.getData().length)){
                logger.log(Level.WARNING, String.format("permissions service rejected publish from [%s] to [%s]", context, topicPath));
                response = registry.getMessageFactory().createPuback(publish.readTopicDataAsInteger(),
                        MqttsnConstants.RETURN_CODE_REJECTED_CONGESTION);
            }
        }

        if(response == null){
            switch (publish.getQoS()) {
                case MqttsnConstants.QoS1:
                    response = registry.getMessageFactory().createPuback(publish.readTopicDataAsInteger(), MqttsnConstants.RETURN_CODE_ACCEPTED);
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

    protected void handlePuback(IMqttsnContext context, IMqttsnMessage originatingMessage, IMqttsnMessage message)
            throws MqttsnException {
    }

    protected IMqttsnMessage handlePubrel(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException {
        return registry.getMessageFactory().createPubcomp();
    }

    protected IMqttsnMessage handlePubrec(IMqttsnContext context, IMqttsnMessage message)
            throws MqttsnException, MqttsnCodecException {
        return registry.getMessageFactory().createPubrel();
    }

    protected void handlePubcomp(IMqttsnContext context, IMqttsnMessage originatingMessage, IMqttsnMessage message)
            throws MqttsnException {
    }

    protected void handleAdvertise(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {

    }

    protected void handleEncapsmsg(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleSearchGw(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return null;
    }

    protected void handleGwinfo(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleWillmsgreq(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return null;
    }

    protected void handleWillmsg(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleWillmsgupd(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return null;
    }

    protected void handleWillmsgresp(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleWilltopicreq(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return null;
    }

    protected void handleWilltopic(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }

    protected IMqttsnMessage handleWilltopicupd(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return null;
    }

    protected void handleWilltopicresp(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
    }
}
