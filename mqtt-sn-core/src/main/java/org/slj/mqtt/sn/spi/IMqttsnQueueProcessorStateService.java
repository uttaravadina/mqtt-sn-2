package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;

public interface IMqttsnQueueProcessorStateService {

    boolean canReceive(IMqttsnContext context) throws MqttsnException;

    void queueEmpty(IMqttsnContext context) throws MqttsnException;
}
