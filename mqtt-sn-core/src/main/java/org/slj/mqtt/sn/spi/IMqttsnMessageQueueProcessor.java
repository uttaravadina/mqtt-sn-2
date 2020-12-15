package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;

/**
 * Uses the installed message queue, when initiated will handle the logic of sending
 * queued messages to the given context, dealing with inflight state, topic registration
 * and backoff
 */
public interface IMqttsnMessageQueueProcessor<T extends IMqttsnRuntimeRegistry>
            extends IMqttsnService<T> {

    boolean process(IMqttsnContext context) throws MqttsnException ;
}
