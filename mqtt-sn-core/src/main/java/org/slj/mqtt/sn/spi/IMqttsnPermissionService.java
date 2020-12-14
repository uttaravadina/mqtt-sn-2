package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;

public interface IMqttsnPermissionService {

    boolean allowConnect(IMqttsnContext context, String clientId) throws MqttsnException;

    boolean allowedToSubscribe(IMqttsnContext context, String topicPath) throws MqttsnException;

    int allowedMaximumQoS(IMqttsnContext context, String topicPath) throws MqttsnException;

    boolean allowedToPublish(IMqttsnContext context, String topicPath, int size) throws MqttsnException;
}
