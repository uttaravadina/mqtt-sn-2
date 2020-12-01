package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.model.MqttsnContext;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.wire.version1_2.payload.MqttsnConnect;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MqttsnContextFactory<T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T> implements IMqttsnContextFactory {

    protected static Logger logger = Logger.getLogger(MqttsnContextFactory.class.getName());

    @Override
    public IMqttsnContext createInitialContext(INetworkContext networkContext, IMqttsnMessage message) throws MqttsnException {
        logger.log(Level.INFO,
                String.format("attempting to identify user and establish mqtt-sn context from [%s]", message));
        if(message instanceof MqttsnConnect){
            MqttsnContext context = new MqttsnContext(networkContext, ((MqttsnConnect)message).getClientId());
            networkContext.setMqttsnContext(context);
            return context;
        }
        throw new MqttsnException("unable to create context message from non-connect packet");

    }
}
