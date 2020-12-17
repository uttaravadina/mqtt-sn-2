package org.slj.mqtt.sn.gateway.impl.gateway;

import org.slj.mqtt.sn.gateway.spi.gateway.IMqttsnGatewayRuntimeRegistry;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.IMqttsnSessionState;
import org.slj.mqtt.sn.model.MqttsnClientState;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.IMqttsnQueueProcessorStateService;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.MqttsnService;
import org.slj.mqtt.sn.utils.MqttsnUtils;

public class MqttsnGatewayQueueProcessorStateService extends MqttsnService<IMqttsnGatewayRuntimeRegistry>
        implements IMqttsnQueueProcessorStateService {

    @Override
    public boolean canReceive(IMqttsnContext context) throws MqttsnException {
        IMqttsnSessionState state = getRegistry().getGatewaySessionService().getSessionState(context, false);
        return state != null && MqttsnUtils.in(state.getClientState() , MqttsnClientState.CONNECTED, MqttsnClientState.AWAKE);
    }

    @Override
    public void queueEmpty(IMqttsnContext context) throws MqttsnException {
        IMqttsnSessionState state = getRegistry().getGatewaySessionService().getSessionState(context, false);
        if(MqttsnUtils.in(state.getClientState() , MqttsnClientState.AWAKE)){
            //-- need to transition the device back to sleep
            getRegistry().getGatewaySessionService().disconnect(state, state.getKeepAlive());
            //-- need to send the closing ping-resp
            IMqttsnMessage pingResp = getRegistry().getMessageFactory().createPingresp();
            getRegistry().getTransport().writeToTransport(context.getNetworkContext(), pingResp);
        }
    }
}
