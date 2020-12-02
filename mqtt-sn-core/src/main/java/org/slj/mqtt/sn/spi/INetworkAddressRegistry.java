package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.net.NetworkAddress;

import java.net.InetAddress;
import java.net.SocketException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface INetworkAddressRegistry {

    INetworkContext getContext(NetworkAddress address);

    INetworkContext getContext(IMqttsnContext context);

    Optional<INetworkContext> first();

    NetworkAddress getNetworkAddress(IMqttsnContext context);

    void putContext(INetworkContext context);

    Optional<INetworkContext> waitForContext(int time, TimeUnit unit) throws InterruptedException;

    List<InetAddress> getAllBroadcastAddresses() throws SocketException;
}
