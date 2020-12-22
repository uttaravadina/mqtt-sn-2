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

    protected Map<LastIdContext, Integer> lastUsedMsgIds;
    protected Map<IMqttsnContext, List<CommitOperation>> uncommittedMessages;
    protected Set<FlushQueueOperation> flushOperations;

    public AbstractMqttsnMessageStateService(boolean clientMode) {
        this.clientMode = clientMode;
    }

    @Override
    public void start(T runtime) throws MqttsnException {
        super.start(runtime);
        uncommittedMessages = Collections.synchronizedMap(new HashMap());
        flushOperations = Collections.synchronizedSet(new HashSet());
        lastUsedMsgIds = Collections.synchronizedMap(new HashMap());
    }

    @Override
    protected boolean doWork() {
        //-- process the messages that have been confirmed inbound on a non application thread
        Iterator<IMqttsnContext> itr = uncommittedMessages.keySet().iterator();
        synchronized (uncommittedMessages) {
            while (itr.hasNext()) {
                IMqttsnContext context = itr.next();
                List<CommitOperation> msgs = uncommittedMessages.get(context);
                Iterator<CommitOperation> msgItr = msgs.iterator();
                synchronized (msgs){
                    while(msgItr.hasNext()){
                        CommitOperation operation = msgItr.next();
                        try {
                            confirmPublish(operation);
                        }
                        catch(Exception e){
                            logger.log(Level.SEVERE, "error committing messages to application ", e);
                        }
                        finally {
                            msgItr.remove();
                        }
                    }
                }
            }
        }

        //-- only use the flush operations when in gateway mode as tje client uses its own thread for this
        Iterator<FlushQueueOperation> flushItr = flushOperations.iterator();
        synchronized (flushOperations) {
            while (flushItr.hasNext()) {
                FlushQueueOperation operation = flushItr.next();

                long delta = (long) Math.pow(2, Math.min(operation.count, 5)) * 250;
                boolean process = operation.timestamp + delta + getRegistry().getOptions().getMinFlushTime() < System.currentTimeMillis();
                if(!process) continue;
                try {
                    IMqttsnMessageQueueProcessor.RESULT result
                            = registry.getQueueProcessor().process(operation.context);
                    operation.timestamp = System.currentTimeMillis();

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
                            break;
                    }

                    logger.log(Level.INFO, String.format("result of process for [%s] was [%s], backoff count was [%s]", operation.context, result, operation.count));
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

        InflightMessage.DIRECTION direction = registry.getMessageHandler().isPartOfOriginatingMessage(message) ?
                InflightMessage.DIRECTION.SENDING : InflightMessage.DIRECTION.RECEIVING;
        int count = countInflight(context, direction);

        if(count > 0){
            logger.log(Level.WARNING,
                    String.format("unable to send [%s],[%s] to [%s], max inflight reached for direction [%s] [%s] -> [%s]",
                            message, queuedPublishMessage, context, direction, count,
                            Objects.toString(getInflightMessages(context))));
            throw new MqttsnExpectationFailedException("fail-fast mode on max inflight max - cap hit");
        }

        if(!allowedToSend(context, message)){
            logger.log(Level.WARNING,
                    String.format("allowed to send [%s] check failed [%s]",
                            message, context));
            throw new MqttsnExpectationFailedException("allowed to send check failed");
        }

        try {

            MqttsnWaitToken token = null;
            boolean requiresResponse = false;
            if((requiresResponse = registry.getMessageHandler().requiresResponse(message))){
                token = markInflight(context, message, queuedPublishMessage);
            }

            logger.log(Level.INFO,
                    String.format("sending message [%s] to [%s] via state service, marking inflight ? [%s]",
                            message, context, requiresResponse));

            registry.getTransport().writeToTransport(context.getNetworkContext(), message);

            //-- the only publish that does not require an ack is QoS so send to app as delivered
            if(!requiresResponse && message instanceof MqttsnPublish){
                CommitOperation op = CommitOperation.outbound(context, queuedPublishMessage.getMessageId(),
                        queuedPublishMessage.getTopicPath(), queuedPublishMessage.getGrantedQoS(),
                        ((MqttsnPublish) message).getData());
                getUncommittedMessages(context).add(op);
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
    public Optional<IMqttsnMessage> waitForCompletion(IMqttsnContext context, final MqttsnWaitToken token, int customWaitTime) throws MqttsnExpectationFailedException {
        try {
            IMqttsnMessage message = token.getMessage();
            if(token.isComplete()){
                return Optional.ofNullable(message);
            }
            IMqttsnMessage response = null;

            long start = System.currentTimeMillis();
            long timeToWait = Math.max(customWaitTime, 0);
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
    public IMqttsnMessage notifyMessageReceived(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException {

        Integer msgId = message.needsMsgId() ? message.getMsgId() : WEAK_ATTACH_ID;
        boolean matchedMessage = inflightExists(context, msgId);
        if (matchedMessage) {
            if (registry.getMessageHandler().isTerminalMessage(message)) {

                InflightMessage inflight = removeInflight(context, msgId);
                logger.log(Level.INFO, String.format("removed terminal message [%s], [%s] => [%s]", msgId, message, inflight));

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
                            token.markComplete();
                            token.notifyAll();
                        }
                    }

                    if (message.isErrorMessage()) {

                        logger.log(Level.WARNING,
                                String.format("requeuing message [%s] in response to [%s] for [%s]",
                                        message, confirmedMessage, context));
                        //received an error message in response, if its requeuable do so
                        if (inflight instanceof RequeueableInflightMessage) {
                            registry.getMessageQueue().offer(context, ((RequeueableInflightMessage) inflight).getQueuedPublishMessage());
                        }

                    } else {
                        logger.log(Level.INFO,
                                String.format("received message [%s] in response to [%s] for [%s], notifying waiting on [%s]",
                                        message, confirmedMessage, context, token));

                        //inbound qos 2 commit
                        if (message instanceof MqttsnPubrel) {
                            CommitOperation op = CommitOperation.inbound(context,
                                    getTopicPathFromPublish(context, (MqttsnPublish) confirmedMessage),
                                    ((MqttsnPublish) confirmedMessage).getQoS(),
                                    ((MqttsnPublish) confirmedMessage).getData());
                            getUncommittedMessages(context).add(op);
                        }

                        //outbound qos 1
                        if (message instanceof MqttsnPuback) {
                            RequeueableInflightMessage rim = (RequeueableInflightMessage) inflight;
                            CommitOperation op = CommitOperation.outbound(context, rim.getQueuedPublishMessage().getMessageId(),
                                    rim.getQueuedPublishMessage().getTopicPath(), rim.getQueuedPublishMessage().getGrantedQoS(),
                                    ((MqttsnPublish) confirmedMessage).getData());
                            getUncommittedMessages(context).add(op);
                        }
                    }
                    return confirmedMessage;
                }
            } else {

                InflightMessage inflight = getInflightMessage(context, msgId);

                logger.log(Level.INFO, String.format("received non terminal message [%s] -> [%s]", msgId, inflight));

                //none terminal matched message.. this is fine (PUBREC or PUBREL)
                //outbound qos 2 commit point
                if(message instanceof MqttsnPubrec){
                    RequeueableInflightMessage rim = (RequeueableInflightMessage) inflight;
                    CommitOperation op = CommitOperation.outbound(context, rim.getQueuedPublishMessage().getMessageId(),
                            rim.getQueuedPublishMessage().getTopicPath(), rim.getQueuedPublishMessage().getGrantedQoS(),
                            ((MqttsnPublish) inflight.getMessage()).getData());
                    getUncommittedMessages(context).add(op);
                }

                return null;
            }

        } else {

            //-- received NEW message that was not associated with an inflight message
            //-- so we need to pin it into the inflight system (if it needs confirming).
            if (message instanceof MqttsnPublish) {
                MqttsnPublish pub = (MqttsnPublish) message;
                if (((MqttsnPublish) message).getQoS() == 2) {
                    int count = countInflight(context, InflightMessage.DIRECTION.RECEIVING);
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
                    logger.log(Level.INFO, String.format("committing inbound publish [%s]", op));
                    getUncommittedMessages(context).add(op);
                }
            }
            return null;
        }
    }

    protected void confirmPublish(CommitOperation operation) {
        try {
            //commit messages to runtime if its a publish
            IMqttsnContext context = operation.context;
            if(operation.inbound){
                registry.getRuntime().messageReceived(context, operation.topicPath, operation.QoS, operation.payload);
            } else {
                registry.getRuntime().messageSent(context, operation.messageId, operation.topicPath, operation.QoS, operation.payload);
            }
        } catch(Exception e){
            logger.log(Level.SEVERE, String.format("error committing message to application;"), e);
        }
    }

    protected MqttsnWaitToken markInflight(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException {

        InflightMessage.DIRECTION direction =
                message instanceof MqttsnPublish ?
                        queuedPublishMessage == null ?
                                InflightMessage.DIRECTION.RECEIVING : InflightMessage.DIRECTION.SENDING :
                                    registry.getMessageHandler().isPartOfOriginatingMessage(message) ?
                                            InflightMessage.DIRECTION.SENDING : InflightMessage.DIRECTION.RECEIVING;

        if(countInflight(context, direction) >=
                registry.getOptions().getMaxMessagesInflight()){
            logger.log(Level.INFO, String.format("[%s] max inflight message number reached, cannot add [%s]", context, message));
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

        logger.log(Level.INFO, String.format("[%s] marking [%s] message [%s] inflight id context [%s]", context,
                direction, message, idContext));

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

        logger.log(Level.INFO, String.format("next id available for context [%s] is [%s]", context, startAt));
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
                    registry.getOptions().getRequeueOnInflightTimeout() && requeueableInflightMessage.getQueuedPublishMessage() != null) {
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

    protected List<CommitOperation> getUncommittedMessages(IMqttsnContext context){
        List<CommitOperation> list = uncommittedMessages.get(context);
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

    @Override
    public void scheduleFlush(IMqttsnContext context) throws MqttsnException {
        if(flushOperations.add(new FlushQueueOperation(context, System.currentTimeMillis()))){
            logger.log(Level.INFO, String.format("added flush for [%s]", context));
        } else {
            //TODO we may want to unbackoff the code when a flush is scheduled
        }
    }

    @Override
    public boolean canSend(IMqttsnContext context) throws MqttsnException {
        return countInflight(context, InflightMessage.DIRECTION.SENDING) == 0;
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
