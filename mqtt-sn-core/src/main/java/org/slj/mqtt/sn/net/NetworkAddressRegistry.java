/*
 * Copyright (c) 2020 Simon Johnson <simon622 AT gmail DOT com>
 *
 * Find me on GitHub:
 * https://github.com/simon622
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.slj.mqtt.sn.net;

import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.spi.INetworkAddressRegistry;
import org.slj.mqtt.sn.spi.NetworkRegistryException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NetworkAddressRegistry implements INetworkAddressRegistry {

    static Logger logger = Logger.getLogger(NetworkAddressRegistry.class.getName());

    final private Map<NetworkAddress, INetworkContext> networkRegistry;
    final private Object mutex = new Object();

    public NetworkAddressRegistry(int initialCapacity){
        networkRegistry = Collections.synchronizedMap(new HashMap<>(initialCapacity));
    }

    public INetworkContext getContext(NetworkAddress address) throws NetworkRegistryException {
        INetworkContext context = networkRegistry.get(address);
        return context;
    }

    public Optional<INetworkContext> first() throws NetworkRegistryException {
        Iterator<NetworkAddress> itr = networkRegistry.keySet().iterator();
        synchronized (networkRegistry){
            while(itr.hasNext()){
                NetworkAddress address = itr.next();
                INetworkContext c = networkRegistry.get(address);
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }

    public void putContext(INetworkContext context) throws NetworkRegistryException {
        networkRegistry.put(context.getNetworkAddress(), context);
        logger.log(Level.INFO, String.format("adding network context to registry - [%s]", context));
        synchronized(mutex){
            mutex.notifyAll();
        }
    }

    public Optional<INetworkContext> waitForContext(int time, TimeUnit unit) throws NetworkRegistryException, InterruptedException {
        synchronized(mutex){
            try {
                while(networkRegistry.isEmpty()){
                    mutex.wait(TimeUnit.MILLISECONDS.convert(time, unit));
                }
                return first();
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

    public List<InetAddress> getAllBroadcastAddresses() throws NetworkRegistryException {
        try {
            List<InetAddress> l = new ArrayList<>();
            Enumeration<NetworkInterface> interfaces
                    = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() ||
                        !networkInterface.isUp()) {
                    continue;
                }
                networkInterface.getInterfaceAddresses().stream()
                        .map(a -> a.getBroadcast())
                        .filter(Objects::nonNull)
                        .forEach(l::add);
            }
            return l;
        } catch(SocketException e){
            throw new NetworkRegistryException(e);
        }
    }
}
