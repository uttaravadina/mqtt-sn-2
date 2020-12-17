package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.model.MqttsnContext;
import org.slj.mqtt.sn.net.NetworkAddress;
import org.slj.mqtt.sn.net.NetworkContext;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnConnect;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MqttsnContextFactory<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T> implements IMqttsnContextFactory {

    protected static Logger logger = Logger.getLogger(MqttsnContextFactory.class.getName());

    @Override
    public INetworkContext createInitialContext(NetworkAddress address) throws MqttsnException {

        logger.log(Level.INFO,
                String.format("not network context exists for address, create new one for [%s]", address));
        return new NetworkContext(address, null);
    }

    @Override
    public IMqttsnContext createInitialContext(INetworkContext networkContext, String clientId) throws MqttsnSecurityException {

        logger.log(Level.INFO,
                String.format("no mqttsn context exists for network context & client id, create new one [%s]", clientId));
        MqttsnContext context = new MqttsnContext(networkContext, clientId);
        networkContext.setMqttsnContext(context);
        return context;
    }
}
