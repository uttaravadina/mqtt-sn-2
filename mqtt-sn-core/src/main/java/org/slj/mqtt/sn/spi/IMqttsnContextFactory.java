package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;

public interface IMqttsnContextFactory <T extends IMqttsnRuntimeRegistry> {

    IMqttsnContext createInitialContext(INetworkContext context, IMqttsnMessage message) throws MqttsnException;

}
