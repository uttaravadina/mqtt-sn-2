package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;

public interface IMqttsnMessageQueueProcessor<T extends IMqttsnRuntimeRegistry>
            extends IMqttsnService<T> {

    void process(IMqttsnContext context) throws MqttsnException ;
}
