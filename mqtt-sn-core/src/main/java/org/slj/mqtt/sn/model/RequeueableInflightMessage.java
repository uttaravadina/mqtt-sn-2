package org.slj.mqtt.sn.model;

import org.slj.mqtt.sn.spi.IMqttsnMessage;

public class RequeueableInflightMessage extends InflightMessage {

    QueuedPublishMessage queuedPublishMessage;

    public RequeueableInflightMessage(QueuedPublishMessage queuedPublishMessage, IMqttsnMessage message, MqttsnWaitToken token) {
        super(message, DIRECTION.SENDING, token);
        this.queuedPublishMessage = queuedPublishMessage;
    }

    public QueuedPublishMessage getQueuedPublishMessage() {
        return queuedPublishMessage;
    }

    public void setQueuedPublishMessage(QueuedPublishMessage queuedPublishMessage) {
        this.queuedPublishMessage = queuedPublishMessage;
    }

    @Override
    public String toString() {
        return "RequeueableInflightMessage{" +
                "token=" + token +
                ", message=" + message +
                ", time=" + time +
                ", queuedPublishMessage=" + queuedPublishMessage +
                '}';
    }
}
