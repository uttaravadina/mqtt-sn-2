package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.net.NetworkAddress;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The network registry maintains a list of known network contexts against a remote address ({@link NetworkAddress}).
 * It exposes functionality to wait for discovered contexts as well as returning a list of valid broadcast addresses.
 */
public interface INetworkAddressRegistry {

    INetworkContext getContext(NetworkAddress address) throws NetworkRegistryException ;

    Optional<INetworkContext> first() throws NetworkRegistryException ;

    void putContext(INetworkContext context) throws NetworkRegistryException ;

    Optional<INetworkContext> waitForContext(int time, TimeUnit unit) throws InterruptedException, NetworkRegistryException;

    List<InetAddress> getAllBroadcastAddresses() throws NetworkRegistryException ;
}
