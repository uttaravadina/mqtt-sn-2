package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.net.NetworkAddress;

public interface IMqttsnContextFactory <T extends IMqttsnRuntimeRegistry> {

    INetworkContext createInitialContext(NetworkAddress address) throws MqttsnException;

    IMqttsnContext createInitialContext(INetworkContext context, String clientId) throws MqttsnSecurityException;

}
