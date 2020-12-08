package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnPublish;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnPubrel;

import java.util.*;
import java.util.logging.Level;

public abstract class AbstractMqttsnMessageStateService <T extends IMqttsnRuntimeRegistry>
        extends AbstractMqttsnBackoffThreadService<T> implements IMqttsnMessageStateService<T> {

    protected static final Integer WEAK_ATTACH_ID = new Integer(MqttsnConstants.USIGNED_MAX_16 + 1);

    protected Map<IMqttsnContext, List<IMqttsnMessage>> uncommittedMessages;

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        uncommittedMessages = Collections.synchronizedMap(new HashMap());
    }

    @Override
    protected boolean doWork() {
        //-- process the messages that have been confirmed inbound on a non application thread
        Iterator<IMqttsnContext> itr = uncommittedMessages.keySet().iterator();
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

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return sendMessageInternal(context, message, null);
    }

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, TopicInfo info, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {

        logger.log(Level.INFO, String.format("sending publish message with id [%s] to [%s]", queuedPublishMessage.getMessageId(), context));
        IMqttsnMessage publish = registry.getMessageFactory().createPublish(queuedPublishMessage.getGrantedQoS(),
                queuedPublishMessage.getRetryCount() > 1, false, info.getType(), info.getTopicId(),
                registry.getMessageRegistry().get(queuedPublishMessage.getMessageId()));
        return sendMessageInternal(context, publish, queuedPublishMessage);
    }

    protected MqttsnWaitToken sendMessageInternal(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {
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
                    token = markInflight(context, message, queuedPublishMessage);
                }
                registry.getTransport().writeToTransport(context, message);

//                when we used to support the wait on queued messages
//                if(!requiresResponse){
//                    //-- if we sent a message that DOESNT require a response, ensure we dont have any threads waiting on it
//                    token = queuedPublishMessage.getToken();
//                    if(token != null){
//                        synchronized (token){
//                            token.markComplete();
//                            token.notifyAll();
//                        }
//                    }
//                }
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
                        message,
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

        Map<Integer, InflightMessage> map = getInflightMessages(context);
        boolean matchedMessage = map.containsKey(message.getMsgId()) ||
                !message.needsMsgId() && map.containsKey(WEAK_ATTACH_ID);

        if(matchedMessage) {
            if (registry.getMessageHandler().isTerminalMessage(message)) {
                InflightMessage msg = removeInflightMessage(context, message.needsMsgId() ? message.getMsgId() : WEAK_ATTACH_ID);
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
                    markInflight(context, message, null);
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
                    logger.log(Level.WARNING, String.format("unable to deliver message to application no topic info!"));
                }
            }
        } catch(Exception e){
            logger.log(Level.SEVERE, String.format("error committing message to application;", e));
        }
    }

    protected MqttsnWaitToken markInflight(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {
        synchronized (context) {
            InflightMessage inflight = queuedPublishMessage == null ? new InflightMessage(message, MqttsnWaitToken.from(message)) :
                    new RequeueableInflightMessage(queuedPublishMessage, message, MqttsnWaitToken.from(message));

            int msgId = WEAK_ATTACH_ID;
            if (message.needsMsgId()) {
                if (message.getMsgId() > 0) {
                    msgId = message.getMsgId();
                } else {
                    msgId = getNextMsgId(context);
                    message.setMsgId(msgId);
                }
            }

            addInflightMessage(context, msgId, inflight);
            return inflight.getToken();
        }
    }

    protected Integer getNextMsgId(IMqttsnContext context) throws MqttsnException {

        Map<Integer, InflightMessage> map = getInflightMessages(context);
        int startAt = Math.max(1, registry.getOptions().getMsgIdStartAt());
        Set<Integer> set = map.keySet();
        for (int i = registry.getOptions().getMsgIdStartAt();
             i < MqttsnConstants.USIGNED_MAX_16; i++){
            if(! set.contains(new Integer(i))){
                return i;
            }
        }
        throw new MqttsnRuntimeException("cannot assign msg id " + startAt);
    }

    protected void clearInflight(IMqttsnContext context, long evictionTime) throws MqttsnException {
        Map<Integer, InflightMessage> messages = getInflightMessages(context);
        if(messages != null && !messages.isEmpty()){
            Iterator<Integer> messageItr = messages.keySet().iterator();
            synchronized (messages){
                while(messageItr.hasNext()){
                    Integer i = messageItr.next();
                    InflightMessage f = messages.get(i);
                    if(f != null){
                        if(evictionTime == 0 || f.getTime() + registry.getOptions().getMaxTimeInflight() < evictionTime){
                            messageItr.remove();
                            reapInflight(context, f);
                        }
                    }
                }
            }
        }
    }

    protected void reapInflight(IMqttsnContext context, InflightMessage inflight) throws MqttsnException {

        IMqttsnMessage message = inflight.getMessage();
        logger.log(Level.WARNING, String.format("clearing message [%s] destined for [%s] aged [%s] from inflight",
                message, context, MqttsnUtils.getDurationString(System.currentTimeMillis() - inflight.getTime())));

        MqttsnWaitToken token = inflight.getToken();
        synchronized (token){
            token.markError();
            token.notifyAll();
        }

        //-- requeue if its a PUBLISH and we have a message queue bound
        if(inflight instanceof RequeueableInflightMessage){
            RequeueableInflightMessage requeueableInflightMessage = (RequeueableInflightMessage) inflight;
            if(registry.getMessageQueue() != null &&
                    registry.getOptions().getRequeueOnInflightTimeout() && requeueableInflightMessage.getQueuedPublishMessage() != null) {
                logger.log(Level.INFO, String.format("re-queuing publish message [%s] for client [%s]", context,
                        inflight.getMessage()));
                registry.getMessageQueue().offer(context, requeueableInflightMessage.getQueuedPublishMessage());
            }
        }
    }

    @Override
    public int countInflight(IMqttsnContext context) throws MqttsnException {
        Map<Integer, InflightMessage> map = getInflightMessages(context);
        return map == null ? 0 : map.size();
    }

    protected List<IMqttsnMessage> getUncommittedMessages(IMqttsnContext context){
        List<IMqttsnMessage> list = uncommittedMessages.get(context);
        if(list == null){
            synchronized (this){
                if((list = uncommittedMessages.get(context)) == null){
                    list = new ArrayList();
                    uncommittedMessages.put(context, list);
                }
            }
        }
        return list;
    }

    protected abstract InflightMessage removeInflightMessage(IMqttsnContext context, Integer messageId) throws MqttsnException ;

    protected abstract void addInflightMessage(IMqttsnContext context, Integer messageId, InflightMessage message) throws MqttsnException ;

    protected abstract Map<Integer, InflightMessage>  getInflightMessages(IMqttsnContext context) throws MqttsnException;
}
