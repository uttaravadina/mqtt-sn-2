package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.model.*;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.*;

import java.util.*;
import java.util.logging.Level;

public abstract class AbstractMqttsnMessageStateService <T extends IMqttsnRuntimeRegistry>
        extends AbstractMqttsnBackoffThreadService<T> implements IMqttsnMessageStateService<T> {

    protected static final Integer WEAK_ATTACH_ID = new Integer(MqttsnConstants.USIGNED_MAX_16 + 1);
    protected boolean clientMode;

    protected Map<IMqttsnContext, Date> lastMessageSent;
    protected Map<LastIdContext, Integer> lastUsedMsgIds;
    protected Set<FlushQueueOperation> flushOperations;

    public AbstractMqttsnMessageStateService(boolean clientMode) {
        this.clientMode = clientMode;
    }

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        flushOperations = Collections.synchronizedSet(new HashSet());
        lastUsedMsgIds = Collections.synchronizedMap(new HashMap());
        lastMessageSent = Collections.synchronizedMap(new HashMap());
    }

    @Override
    protected boolean doWork() {
        //-- only use the flush operations when in gateway mode as the client uses its own thread for this
        Iterator<FlushQueueOperation> flushItr = flushOperations.iterator();
        synchronized (flushOperations) {
            while (flushItr.hasNext()) {
                FlushQueueOperation operation = flushItr.next();
                long delta = (long) Math.pow(1.7, Math.min(operation.count, 8)) * 25;
                long processAfter = operation.timestamp + Math.max(delta, getRegistry().getOptions().getMinFlushTime());
                boolean process = processAfter <= System.currentTimeMillis();
                if(!process){
                    logger.log(Level.FINE, String.format("back off [%s] for [%s] or [%s] -> missed by [%s]",
                            operation.count, delta, getRegistry().getOptions().getMinFlushTime(), processAfter - System.currentTimeMillis()));
                    continue;
                }

                try {
                    IMqttsnMessageQueueProcessor.RESULT result
                            = registry.getQueueProcessor().process(operation.context);

                    switch(result){
                        case REMOVE_PROCESS:
                            logger.log(Level.INFO, String.format("removing context from flush", operation.context));
                            flushItr.remove();
                            break;
                        case REPROCESS:
                            //knock the time on for another attempt
                            operation.count = 0;
                            break;
                        case BACKOFF_PROCESS:
                            //knock the time on for another attempt
                            operation.count++;
                            operation.timestamp = System.currentTimeMillis();
                            break;
                    }

                    if(logger.isLoggable(Level.FINE)){
                        logger.log(Level.FINE,
                                String.format("result of process for [%s] was [%s], backoff count was [%s]", operation.context, result, operation.count));
                    }
                } catch(Exception e){
                    logger.log(Level.SEVERE, "error flushing context on state thread;", e);
                }
            }
        }
        try {
            registry.getMessageRegistry().tidy();
        } catch(Exception e){
            logger.log(Level.SEVERE, "error tidying message registry on state thread;", e);
        }

        return true;
    }

    protected boolean allowedToSend(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return true;
    }

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {
        return sendMessageInternal(context, message, null);
    }

    @Override
    public MqttsnWaitToken sendMessage(IMqttsnContext context, TopicInfo info, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {

        byte[] payload = registry.getMessageRegistry().get(queuedPublishMessage.getMessageId());
        IMqttsnMessage publish = registry.getMessageFactory().createPublish(queuedPublishMessage.getGrantedQoS(),
                queuedPublishMessage.getRetryCount() > 1, false, info.getType(),  info.getTopicId(),
                payload);
        return sendMessageInternal(context, publish, queuedPublishMessage);
    }

    protected MqttsnWaitToken sendMessageInternal(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {

        if(!allowedToSend(context, message)){
            logger.log(Level.WARNING,
                    String.format("allowed to send [%s] check failed [%s]",
                            message, context));
            throw new MqttsnExpectationFailedException("allowed to send check failed");
        }

        InflightMessage.DIRECTION direction = registry.getMessageHandler().isPartOfOriginatingMessage(message) ?
                InflightMessage.DIRECTION.SENDING : InflightMessage.DIRECTION.RECEIVING;

        int count = countInflight(context, direction);
        if(count > 0){
            logger.log(Level.WARNING,
                    String.format("presently unable to send [%s],[%s] to [%s], max inflight reached for direction [%s] [%s] -> [%s]",
                            message, queuedPublishMessage, context, direction, count,
                            Objects.toString(getInflightMessages(context))));

            Optional<InflightMessage> blockingMessage =
                    getInflightMessages(context).values().stream().filter(i -> i.getDirection() == direction).findFirst();
            if(blockingMessage.isPresent() && clientMode){
                //-- if we are in client mode, attempt to wait for the ongoing outbound message to complete before we issue next message
                MqttsnWaitToken token = blockingMessage.get().getToken();
                if(token != null){
                    waitForCompletion(context, token);
                    //-- recurse point
                    return sendMessageInternal(context, message, queuedPublishMessage);
                }
            } else {
                throw new MqttsnExpectationFailedException("max number of inflight messages reached");
            }
        }

        try {

            MqttsnWaitToken token = null;
            boolean requiresResponse = false;
            if((requiresResponse = registry.getMessageHandler().requiresResponse(message))){
                token = markInflight(context, message, queuedPublishMessage);
            }

            if(logger.isLoggable(Level.FINE)){
                logger.log(Level.FINE,
                        String.format("sending message [%s] to [%s] via state service, marking inflight ? [%s]",
                                message, context, requiresResponse));
            }

            registry.getTransport().writeToTransport(context.getNetworkContext(), message);
            lastMessageSent.put(context, new Date());

            //-- the only publish that does not require an ack is QoS so send to app as delivered
            if(!requiresResponse && message instanceof MqttsnPublish){
                CommitOperation op = CommitOperation.outbound(context, queuedPublishMessage.getMessageId(),
                        queuedPublishMessage.getTopicPath(), queuedPublishMessage.getGrantedQoS(),
                        ((MqttsnPublish) message).getData());
                confirmPublish(op);
            }

            return token;

        } catch(Exception e){
            throw new MqttsnException("error sending message with confirmations", e);
        }
    }

    @Override
    public Optional<IMqttsnMessage> waitForCompletion(IMqttsnContext context, final MqttsnWaitToken token) throws MqttsnExpectationFailedException {
        return waitForCompletion(context, token, registry.getOptions().getMaxWait());
    }

    @Override
    public Optional<IMqttsnMessage> waitForCompletion(IMqttsnContext context, final MqttsnWaitToken token, int waitTime) throws MqttsnExpectationFailedException {
        try {
            IMqttsnMessage message = token.getMessage();
            if(token.isComplete()){
                return Optional.ofNullable(message);
            }
            IMqttsnMessage response = null;

            long start = System.currentTimeMillis();
            long timeToWait = Math.max(waitTime, 0);
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
                    logger.log(Level.INFO, String.format("token [%s] in [%s], confirmation of message [%s] -> [%s]",
                            token.isError() ? "error" : "ok", MqttsnUtils.getDurationString(time), context, response == null ? "<null>" : response));
                }
                return Optional.ofNullable(response);
            } else {
                logger.log(Level.WARNING, String.format("token timed out waiting [%s]ms for response to [%s] in [%s]",
                        waitTime,
                        message,
                        MqttsnUtils.getDurationString(time)));
                token.markError();
                throw new MqttsnExpectationFailedException("unable to obtain response within timeout ("+waitTime+")");
            }

        } catch(InterruptedException e){
            logger.log(Level.WARNING, "a thread waiting for a message being sent was interrupted;", e);
            Thread.currentThread().interrupt();
            throw new MqttsnRuntimeException(e);
        }
    }

    @Override
    public IMqttsnMessage notifyMessageReceived(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {

        Integer msgId = message.needsMsgId() ? message.getMsgId() : WEAK_ATTACH_ID;
        boolean matchedMessage = inflightExists(context, msgId);
        boolean terminalMessage = registry.getMessageHandler().isTerminalMessage(message);
        if (matchedMessage) {
            if (terminalMessage) {
                InflightMessage inflight = removeInflight(context, msgId);
                if (!registry.getMessageHandler().validResponse(inflight.getMessage(), message.getClass())) {
                    logger.log(Level.WARNING,
                            String.format("invalid response message [%s] for [%s] -> [%s]",
                                    message, inflight.getMessage(), context));
                    throw new MqttsnRuntimeException("invalid response received " + message.getMessageName());
                } else {

                    IMqttsnMessage confirmedMessage = inflight.getMessage();
                    MqttsnWaitToken token = inflight.getToken();

                    if (token != null) {
                        synchronized (token) {
                            //-- release any waits
                            token.setResponseMessage(message);
                            if (message.isErrorMessage()) token.markError();
                            else token.markComplete();
                            token.notifyAll();
                        }
                    }

                    if (message.isErrorMessage()) {

                        logger.log(Level.WARNING,
                                String.format("error response received [%s] in response to [%s] for [%s]",
                                        message, confirmedMessage, context));

                        //received an error message in response, if its requeuable do so
                        if (inflight instanceof RequeueableInflightMessage) {
                            logger.log(Level.INFO,
                                    String.format("message was re-queueable offer to queue [%s]", context));
                            registry.getMessageQueue().offer(context, ((RequeueableInflightMessage) inflight).getQueuedPublishMessage());
                        }

                    } else {

                        //inbound qos 2 commit
                        if (message instanceof MqttsnPubrel) {
                            CommitOperation op = CommitOperation.inbound(context,
                                    getTopicPathFromPublish(context, (MqttsnPublish) confirmedMessage),
                                    ((MqttsnPublish) confirmedMessage).getQoS(),
                                    ((MqttsnPublish) confirmedMessage).getData());
                            confirmPublish(op);
                        }

                        //outbound qos 1
                        if (message instanceof MqttsnPuback) {
                            RequeueableInflightMessage rim = (RequeueableInflightMessage) inflight;
                            CommitOperation op = CommitOperation.outbound(context, rim.getQueuedPublishMessage().getMessageId(),
                                    rim.getQueuedPublishMessage().getTopicPath(), rim.getQueuedPublishMessage().getGrantedQoS(),
                                    ((MqttsnPublish) confirmedMessage).getData());
                            confirmPublish(op);
                        }
                    }
                    return confirmedMessage;
                }
            } else {

                InflightMessage inflight = getInflightMessage(context, msgId);

                //none terminal matched message.. this is fine (PUBREC or PUBREL)
                //outbound qos 2 commit point
                if(message instanceof MqttsnPubrec){
                    RequeueableInflightMessage rim = (RequeueableInflightMessage) inflight;
                    CommitOperation op = CommitOperation.outbound(context, rim.getQueuedPublishMessage().getMessageId(),
                            rim.getQueuedPublishMessage().getTopicPath(), rim.getQueuedPublishMessage().getGrantedQoS(),
                            ((MqttsnPublish) inflight.getMessage()).getData());
                    confirmPublish(op);
                }

                return null;
            }

        } else {

            //-- received NEW message that was not associated with an inflight message
            //-- so we need to pin it into the inflight system (if it needs confirming).
            if (message instanceof MqttsnPublish) {
                MqttsnPublish pub = (MqttsnPublish) message;
                if (((MqttsnPublish) message).getQoS() == 2) {
//                    int count = countInflight(context, InflightMessage.DIRECTION.RECEIVING);
//                    if(count >= registry.getOptions().getMaxMessagesInflight()){
//                        logger.log(Level.WARNING, String.format("have [%s] existing inbound message(s) inflight & new publish QoS2, replacing inflights!", count));
//                       throw new MqttsnException("cannot receive more than maxInflight!");
//                    }
                    //-- Qos 2 needs further confirmation before being sent to application
                    markInflight(context, message, null);
                } else {
                    //-- Qos 0 & 1 are inbound are confirmed on receipt of message

                    CommitOperation op = CommitOperation.inbound(context,
                            getTopicPathFromPublish(context, pub),
                            ((MqttsnPublish) message).getQoS(),
                            ((MqttsnPublish) message).getData());
                    confirmPublish(op);
                }
            }
            return null;
        }
    }

    /**
     * Confirmation delivery to the application takes place on the worker thread group
     */
    protected void confirmPublish(final CommitOperation operation) {
        getRegistry().getRuntime().async(() -> {
            IMqttsnContext context = operation.context;
            if(operation.inbound){
                registry.getRuntime().messageReceived(context, operation.topicPath, operation.QoS, operation.payload);
            } else {
                registry.getRuntime().messageSent(context, operation.messageId, operation.topicPath, operation.QoS, operation.payload);
            }
        });
    }

    protected MqttsnWaitToken markInflight(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage)
            throws MqttsnException {

        InflightMessage.DIRECTION direction =
                message instanceof MqttsnPublish ?
                        queuedPublishMessage == null ?
                                InflightMessage.DIRECTION.RECEIVING : InflightMessage.DIRECTION.SENDING :
                                    registry.getMessageHandler().isPartOfOriginatingMessage(message) ?
                                            InflightMessage.DIRECTION.SENDING : InflightMessage.DIRECTION.RECEIVING;

        if(countInflight(context, direction) >=
                registry.getOptions().getMaxMessagesInflight()){
            logger.log(Level.WARNING, String.format("[%s] max inflight message number reached, either wait of fail-fast [%s]", context, message));
            throw new MqttsnExpectationFailedException("max number of inflight messages reached");
        }

        InflightMessage inflight = queuedPublishMessage == null ? new InflightMessage(message, direction, MqttsnWaitToken.from(message)) :
                new RequeueableInflightMessage(queuedPublishMessage, message, MqttsnWaitToken.from(message));

        LastIdContext idContext = LastIdContext.from(context, direction);
        int msgId = WEAK_ATTACH_ID;
        if (message.needsMsgId()) {
            if (message.getMsgId() > 0) {
                msgId = message.getMsgId();
            } else {
                msgId = getNextMsgId(idContext);
                message.setMsgId(msgId);
            }
        }

        addInflightMessage(context, msgId, inflight);

        if(logger.isLoggable(Level.FINE)){
            logger.log(Level.FINE, String.format("[%s] marking [%s] message [%s] inflight id context [%s]", context,
                    direction, message, idContext));
        }

        if(msgId != WEAK_ATTACH_ID) lastUsedMsgIds.put(idContext, msgId);
        return inflight.getToken();
    }

    protected Integer getNextMsgId(LastIdContext context) throws MqttsnException {

        Map<Integer, InflightMessage> map = getInflightMessages(context.context);
        int startAt = Math.max(lastUsedMsgIds.get(context) == null ? 1 : lastUsedMsgIds.get(context) + 1,
                registry.getOptions().getMsgIdStartAt());

        Set<Integer> set = map.keySet();
        while(set.contains(new Integer(startAt))){
            startAt = ++startAt % MqttsnConstants.USIGNED_MAX_16;
        }

        if(set.contains(new Integer(startAt)))
            throw new MqttsnRuntimeException("cannot assign msg id " + startAt);

        if(logger.isLoggable(Level.FINE)){
            logger.log(Level.FINE, String.format("next id available for context [%s] is [%s]", context, startAt));
        }

        return startAt;
    }

    public void clearInflight(IMqttsnContext context) throws MqttsnException {
        clearInflightInternal(context, 0);
        clear(context);
    }

    protected void clearInflightInternal(IMqttsnContext context, long evictionTime) throws MqttsnException {
        logger.log(evictionTime == 0 ? Level.INFO : Level.FINE, String.format("clearing all inflight messages for context [%s], forced = [%s]", context, evictionTime == 0));
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
                    registry.getOptions().isRequeueOnInflightTimeout() &&
                    requeueableInflightMessage.getQueuedPublishMessage() != null) {
                logger.log(Level.INFO, String.format("re-queuing publish message [%s] for client [%s]", context,
                        inflight.getMessage()));
                registry.getMessageQueue().offer(context, requeueableInflightMessage.getQueuedPublishMessage());
            }
        }
    }

    @Override
    public int countInflight(IMqttsnContext context, InflightMessage.DIRECTION direction) throws MqttsnException {
        Map<Integer, InflightMessage> map = getInflightMessages(context);
        Iterator<Integer> itr = map.keySet().iterator();
        int count = 0;
        while(itr.hasNext()){
            Integer i = itr.next();
            InflightMessage msg = map.get(i);
            if(msg.getDirection() == direction) count++;
        }
        return count;
    }

    @Override
    public void scheduleFlush(IMqttsnContext context) throws MqttsnException {
        if(flushOperations.add(new FlushQueueOperation(context, System.currentTimeMillis()))){
            if(logger.isLoggable(Level.FINE)){
                logger.log(Level.FINE, String.format("added flush for [%s]", context));
            }
        } else {
            //TODO this is nasty - we need a fast lookup now weve introduced expediting processor
            Iterator<FlushQueueOperation> itr = flushOperations.iterator();
            while(itr.hasNext()){
                FlushQueueOperation op = itr.next();
                if(op.context.equals(context)){
                    op.resetCount();
                    if(logger.isLoggable(Level.INFO)){
                        logger.log(Level.INFO, String.format("reset flush backoff for [%s]", context));
                    }
                }
            }
        }
        expedite();
    }

    @Override
    public boolean canSend(IMqttsnContext context) throws MqttsnException {
        return countInflight(context, InflightMessage.DIRECTION.SENDING) == 0;
    }

    @Override
    public Date getMessageLastSentToContext(IMqttsnContext context) {
        return lastMessageSent.get(context);
    }

    protected String getTopicPathFromPublish(IMqttsnContext context, MqttsnPublish publish) throws MqttsnException {
        TopicInfo info = registry.getTopicRegistry().normalize((byte) publish.getTopicType(), publish.getTopicData(), false);
        String topicPath = registry.getTopicRegistry().topicPath(context, info, true);
        return topicPath;
    }

    protected abstract void addInflightMessage(IMqttsnContext context, Integer messageId, InflightMessage message) throws MqttsnException ;

    protected abstract InflightMessage getInflightMessage(IMqttsnContext context, Integer messageId) throws MqttsnException ;

    protected abstract Map<Integer, InflightMessage>  getInflightMessages(IMqttsnContext context) throws MqttsnException;

    protected abstract boolean inflightExists(IMqttsnContext context, Integer messageId) throws MqttsnException;

    static class CommitOperation {

        protected int QoS;
        protected String topicPath;
        protected byte[] payload;
        protected IMqttsnContext context;
        protected long timestamp;
        protected UUID messageId;
        protected boolean inbound = true;

        public CommitOperation(IMqttsnContext context, long timestamp, String topicPath, int QoS, byte[] payload, boolean inbound) {
            this.context = context;
            this.timestamp = timestamp;
            this.inbound = inbound;
            this.topicPath = topicPath;
            this.payload = payload;
            this.QoS = QoS;
        }

        public static CommitOperation inbound(IMqttsnContext context, String topicPath, int QoS, byte[] payload){
            return new CommitOperation(context, System.currentTimeMillis(), topicPath, QoS, payload, true);
        }

        public static CommitOperation outbound(IMqttsnContext context, UUID messageId, String topicPath, int QoS, byte[] payload){
            CommitOperation c = new CommitOperation(context, System.currentTimeMillis(), topicPath, QoS, payload, false);
            c.messageId = messageId;
            return c;
        }
    }

    static class FlushQueueOperation {

        protected final IMqttsnContext context;
        protected long timestamp;
        protected int count;

        public FlushQueueOperation(IMqttsnContext context, long timestamp){
            this.context = context;
            this.timestamp = timestamp;
        }

        public void resetCount(){
            count = 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FlushQueueOperation that = (FlushQueueOperation) o;
            return Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context);
        }
    }

    static class LastIdContext {

        protected final IMqttsnContext context;
        protected final InflightMessage.DIRECTION direction;

        public LastIdContext(IMqttsnContext context, InflightMessage.DIRECTION direction) {
            this.context = context;
            this.direction = direction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LastIdContext that = (LastIdContext) o;
            return context.equals(that.context) && direction == that.direction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, direction);
        }

        @Override
        public String toString() {
            return "LastIdContext{" +
                    "context=" + context +
                    ", direction=" + direction +
                    '}';
        }

        public static LastIdContext from(IMqttsnContext context, InflightMessage.DIRECTION direction){
            return new LastIdContext(context, direction);
        }
    }
}
