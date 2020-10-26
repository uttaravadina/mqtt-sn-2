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
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.impl.AbstractMqttsnBackoffThreadService;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnPublish;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnPubrel;

import java.util.*;
import java.util.logging.Level;

public class MqttsnInMemoryMessageStateService<T extends IMqttsnRuntimeRegistry>
        extends AbstractMqttsnBackoffThreadService<T> implements IMqttsnMessageStateService<T> {

    protected static final Integer WEAK_ATTACH_ID = new Integer(MqttsnConstants.USIGNED_MAX_16 + 1);

    protected Map<IMqttsnContext, Integer> previousId;
    protected Map<IMqttsnContext, Map<Integer, InflightMessage>> inflightMessages;
    protected Map<IMqttsnContext, List<IMqttsnMessage>> uncommittedMessages;

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        previousId = Collections.synchronizedMap(new HashMap());
        inflightMessages = Collections.synchronizedMap(new HashMap());
        uncommittedMessages = Collections.synchronizedMap(new HashMap());
    }

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return sendMessage(context, message, null);
    }

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {
        try {
            synchronized (context){
                MqttsnWaitToken token = null;
                int count = countInflight(context);
                if(count >=
                        registry.getOptions().getMaxMessagesInflight()){
                    logger.log(Level.WARNING,
                            String.format("unable to send [%s],[%s] to [%s], max inflight reached [%s]",
                                    message, queuedPublishMessage, context, count));
                    throw new MqttsnExpectationFailedException("fail-fast mode on max inflight max - cap hit");
                }

                logger.log(Level.INFO,
                        String.format("sending message [%s] to [%s] via state service",
                                message, context));
                boolean requiresResponse = false;
                if((requiresResponse = registry.getMessageHandler().requiresResponse(message))){
                    token = attach(context, message, queuedPublishMessage, false);
                }
                registry.getTransport().writeToTransport(context, message);

                if(queuedPublishMessage != null && !requiresResponse){
                    //-- if we sent a message that DOESNT require a response, ensure we dont have any threads waiting on it
                    token = queuedPublishMessage.getToken();
                    synchronized (token){
                        token.markComplete();
                        token.notifyAll();
                    }
                }
                return token;
            }
        } catch(Exception e){
            throw new MqttsnException("error sending message with confirmations", e);
        }
    }

    @Override
    public Optional<IMqttsnMessage> waitForCompletion(IMqttsnContext context, final MqttsnWaitToken token) throws MqttsnExpectationFailedException {
        try {
            IMqttsnMessage message = token.getMessage();
            if(token.isComplete()){
                return Optional.ofNullable(message);
            }
            IMqttsnMessage response = null;

            long start = System.currentTimeMillis();
            long timeToWait = registry.getOptions().getMaxWait();
            synchronized(token){
                //-- code against spurious wake up
                while(!token.isComplete() &&
                        timeToWait > System.currentTimeMillis() - start) {
                    token.wait(timeToWait);
                }
            }

            long time = System.currentTimeMillis() - start;
            if(token.isComplete()){
                response = token.getResponseMessage();
                if(logger.isLoggable(Level.INFO)){
                    logger.log(Level.INFO, String.format("token [%s] finished ok, confirmation of message [%s] -> [%s]",
                            MqttsnUtils.getDurationString(time), context, response == null ? "<null>" : response));
                }
                return Optional.ofNullable(response);
            } else {
                logger.log(Level.WARNING, String.format("token timed out waiting for response to [%s] in [%s]",
                        message == null ? token.getPublishMessage() : message,
                        MqttsnUtils.getDurationString(time)));
                token.markError();
                throw new MqttsnExpectationFailedException("unable to obtain response within timeout");
            }

        } catch(InterruptedException e){
            logger.log(Level.WARNING, "a thread waiting for a message being sent was interrupted;", e);
            Thread.currentThread().interrupt();
            throw new MqttsnRuntimeException(e);
        }
    }

    @Override
    public IMqttsnMessage notifyMessageReceived(IMqttsnContext context, IMqttsnMessage message, boolean evictExistingInflight) throws MqttsnException {

        Map<Integer, InflightMessage> map = getLookup(context);
        boolean matchedMessage = map.containsKey(message.getMsgId()) ||
                !message.needsMsgId() && map.containsKey(WEAK_ATTACH_ID);

        if(matchedMessage) {
            if (registry.getMessageHandler().isTerminalMessage(message)) {
                InflightMessage msg = map.remove(message.needsMsgId() ? message.getMsgId() : WEAK_ATTACH_ID);
                if(!registry.getMessageHandler().validResponse(msg.getMessage(), message.getClass())){
                    logger.log(Level.WARNING,
                            String.format("invalid response message [%s] for [%s] -> [%s]",
                                    message, msg.getMessage(), context));
                    throw new MqttsnRuntimeException("invalid response received " + message.getMessageName());
                } else {

                    IMqttsnMessage confirmedMessage = msg.getMessage();
                    logger.log(Level.INFO,
                                String.format("received message [%s] in response to [%s] for [%s], notifying waiting objects",
                                        message, confirmedMessage, context));

                    synchronized (msg.getToken()) {
                        //-- release any waits
                        MqttsnWaitToken token = msg.getToken();
                        token.setResponseMessage(message);
                        token.markComplete();
                        token.notifyAll();
                        if(message instanceof MqttsnPubrel){
                            //PUB REL IS THE COMMIT POINT FOR RECEIVED
                            //QOS2 messages
                            receiveConfirmedInboundPublish(context, confirmedMessage);
                        }
                    }
                    return confirmedMessage;
                }
            }
            else {
                //-- none terminal matched message.. this is fine (PUBREC or PUBREL)
                logger.log(Level.INFO, String.format("intermediary confirmation message received [%s]", message));
                return null;
            }
        } else {

            //-- received NEW message that was not associated with an inflight message
            //-- so we need to pin it into the inflight system (if it needs confirming).
            if(message instanceof MqttsnPublish){
                if(((MqttsnPublish)message).getQoS() == 2){
                    //-- Qos 2 needs further confirmation before being sent to application
                    attach(context, message, null, evictExistingInflight);
                } else {
                    //-- Qos 0 & 1 are confirmed on receipt of message
                    receiveConfirmedInboundPublish(context, message);
                }
            }
            return null;
        }
    }

    /**
     * Messages received and confirmed through the state service should NOT block the
     * transport layer notifying the application of inbound messages, so pre-commit them
     * for processing on the main worker thread
     */
    protected void receiveConfirmedInboundPublish(IMqttsnContext context, IMqttsnMessage message){
        if(message instanceof MqttsnPublish){
            MqttsnPublish publish = (MqttsnPublish) message;
            List<IMqttsnMessage> list = getUncommittedMessages(context);
            list.add(publish);
        } else {
            throw new MqttsnRuntimeException("unable to received confirmed non-publish message");
        }
    }

    protected void confirmInboundPublish(IMqttsnContext context, IMqttsnMessage message) {
        try {
            //commit messages to runtime if its a publish
            if(message instanceof MqttsnPublish){
                MqttsnPublish publish = (MqttsnPublish) message;
                TopicInfo info =
                        registry.getTopicRegistry().normalize((byte) publish.getTopicType(), publish.getTopicData(), false);
                if(info != null){
                    String topicPath = registry.getTopicRegistry().topicPath(context, info, true);
                    registry.getRuntime().messageReceived(context, topicPath, publish.getQoS(), publish.getData());
                } else {
                    logger.log(Level.WARNING, String.format("unable to delivery message to application no topic info!"));
                }
            }
        } catch(Exception e){
            logger.log(Level.SEVERE, String.format("error committing message to application;", e));
        }
    }

    protected MqttsnWaitToken attach(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage, boolean evictExistingInflight) throws MqttsnException {
        synchronized (context){
            Map<Integer, InflightMessage> map = getLookup(context);
            if(evictExistingInflight && maxInflightMessageCountReached(context, map)){
                clearInflight(context, 0);
                logger.log(Level.WARNING, String.format("[%s] evicting inflight, giving priority to received message [%s]", context, message));
            }

            if(message.needsMsgId()){
                return _confirmedAttach(context, message, queuedPublishMessage);
            } else {
                return _weakAttach(context, message);
            }
        }
    }

    protected MqttsnWaitToken _weakAttach(IMqttsnContext context, IMqttsnMessage msg) throws MqttsnException {

        Map<Integer, InflightMessage> map = getLookup(context);
        validateInflightMessageCount(context, map);
        MqttsnWaitToken token = MqttsnWaitToken.from(msg);
        map.put(WEAK_ATTACH_ID, new InflightMessage(context, msg, token));
        logger.log(Level.INFO, String.format("attaching client message [%s] -> [%s]", WEAK_ATTACH_ID, msg == null ? "<null>" : msg));
        return token;
    }

    protected MqttsnWaitToken _confirmedAttach(IMqttsnContext context, IMqttsnMessage msg, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {

        Map<Integer, InflightMessage> map = getLookup(context);
        validateInflightMessageCount(context, map);
        //-- either create a new token from the message OR, if this message is as a result of a queued publish, use the token
        //-- created by that
        MqttsnWaitToken token = queuedPublishMessage != null ? queuedPublishMessage.getToken() : MqttsnWaitToken.from(msg);
        try {
            int msgId = 0;
            if(msg.getMsgId() > 0){
                msgId = msg.getMsgId();
            } else {
                msgId = getNextMsgId(context);
                msg.setMsgId(msgId);
            }
            logger.log(Level.INFO, String.format("attaching client message [%s] -> [%s]", msgId, msg == null ? "<null>" : msg));
            if(queuedPublishMessage != null){
                //join the new protocol message to the queued message for locking
                token.setMessage(msg);
            }
            map.put(msgId, new InflightMessage(context, msg, queuedPublishMessage, token));
            return token;
        } catch(MqttsnCodecException e){
            throw new MqttsnException("error setting message id on message", e);
        }
    }

    protected void validateInflightMessageCount(IMqttsnContext context, Map<Integer, InflightMessage> map) throws MqttsnException{
        if(maxInflightMessageCountReached(context, map)){
            throw new MqttsnException("max number of messages inflight reached for client");
        }
    }

    protected boolean maxInflightMessageCountReached(IMqttsnContext context, Map<Integer, InflightMessage> map) throws MqttsnException{
        if(map.size() >= registry.getOptions().getMaxMessagesInflight()){
            logger.log(Level.WARNING, String.format("max number of messages inflight reached for client [%s] -> [%s]", context, map.size()));
            return true;
        }
        return false;
    }

    protected Integer getNextMsgId(IMqttsnContext context) {

        Map<Integer, InflightMessage> map = getLookup(context);

        int startAt = Math.max(1, registry.getOptions().getMsgIdStartAt());
        Integer pId = null;
        if((pId = previousId.get(context)) != null){
            startAt = (pId.intValue() + 1) % 0xFFFF;
            startAt = Math.max(startAt, registry.getOptions().getMsgIdStartAt());
        }

        if(map.containsKey(startAt)) throw new MqttsnRuntimeException("cannot assign msg id " + startAt);
        previousId.put(context, new Integer(startAt));
        return startAt;
    }

    protected Map<Integer, InflightMessage> getLookup(IMqttsnContext context){
        Map<Integer, InflightMessage> map = inflightMessages.get(context);
        if(map == null){
            synchronized (this){
                if((map = inflightMessages.get(context)) == null){
                    map = createInflightLookup(context);
                    inflightMessages.put(context, map);
                }
            }
        }
        return map;
    }

    protected List<IMqttsnMessage> getUncommittedMessages(IMqttsnContext context){
        List<IMqttsnMessage> list = uncommittedMessages.get(context);
        if(list == null){
            synchronized (this){
                if((list = uncommittedMessages.get(context)) == null){
                    list = createUncommittedBag(context);
                    uncommittedMessages.put(context, list);
                }
            }
        }
        return list;
    }

    protected Map<Integer, InflightMessage> createInflightLookup(IMqttsnContext context){
        return new HashMap<>();
    }

    protected List createUncommittedBag(IMqttsnContext context){
        return Collections.synchronizedList(new ArrayList<>());
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

        itr = uncommittedMessages.keySet().iterator();
        synchronized (uncommittedMessages) {
            while (itr.hasNext()) {
                IMqttsnContext context = itr.next();
                List<IMqttsnMessage> msgs = uncommittedMessages.get(context);
                Iterator<IMqttsnMessage> msgItr = msgs.iterator();
                synchronized (msgs){
                    while(msgItr.hasNext()){
                        confirmInboundPublish(context, msgItr.next());
                        msgItr.remove();
                    }
                }
            }
        }
        return true;
    }

    protected void clearInflight(IMqttsnContext context, long evictionTime) throws MqttsnException {
        Map<Integer, InflightMessage> messages = inflightMessages.get(context);
        if(messages != null && !messages.isEmpty()){
            Iterator<Integer> messageItr = messages.keySet().iterator();
            synchronized (messages){
                while(messageItr.hasNext()){
                    Integer i = messageItr.next();
                    InflightMessage f = messages.get(i);
                    if(f != null){
                        if(evictionTime == 0 || f.getTime() + registry.getOptions().getMaxTimeInflight() < evictionTime){
                            messageItr.remove();
                            reapInflight(f);
                        }
                    }
                }
            }
        }
    }

    protected void reapInflight(InflightMessage inflight) throws MqttsnException {

        IMqttsnMessage message = inflight.getMessage();
        logger.log(Level.WARNING, String.format("clearing message [%s] destined for [%s] aged [%s] from inflight",
                message, inflight.getContext(), MqttsnUtils.getDurationString(System.currentTimeMillis() - inflight.getTime())));

        MqttsnWaitToken token = inflight.getToken();
        synchronized (token){
            token.markError();
            token.notifyAll();
        }

        //-- requeue if its a PUBLISH and we have a message queue bound
        if(message instanceof MqttsnPublish){
            if(registry.getMessageQueue() != null &&
                registry.getOptions().getRequeueOnInflightTimeout() && inflight.getQueuedMessage() != null) {

                logger.log(Level.INFO, String.format("re-queuing publish message [%s] for client [%s]", inflight.getContext(),
                        inflight.getMessage()));
                registry.getMessageQueue().offer(inflight.getContext(), inflight.getQueuedMessage());
            }
        } else {
            logger.log(Level.INFO, String.format("discarding non-publish message [%s] for client [%s]", inflight.getContext(), inflight.getMessage()));
        }
    }

    @Override
    public int countInflight(IMqttsnContext context) {
        Map<Integer, InflightMessage> map = getLookup(context);
        return map == null ? 0 : map.size();
    }

    protected MqttsnWaitToken waitingOn(IMqttsnContext context) {
        Map<Integer, InflightMessage> map = getLookup(context);
        if(map == null) return null;
        TreeSet<Integer> sortedIds = null;
        synchronized (map){
            sortedIds = new TreeSet<>(map.keySet());
        }
        Integer i = sortedIds.first();
        if(i != null){
            InflightMessage m = map.get(i);
            return m == null ? null : m.getToken();
        }
        return null;
    }

    @Override
    public void clear(IMqttsnContext context) {
        inflightMessages.remove(context);
    }

    @Override
    public void clearAll() {
        inflightMessages.clear();
    }
}
