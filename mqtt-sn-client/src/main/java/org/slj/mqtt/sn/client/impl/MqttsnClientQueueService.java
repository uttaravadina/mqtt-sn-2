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

package org.slj.mqtt.sn.client.impl;

import org.slj.mqtt.sn.client.spi.IMqttsnClientQueueService;
import org.slj.mqtt.sn.client.spi.IMqttsnClientRuntimeRegistry;
import org.slj.mqtt.sn.impl.AbstractMqttsnBackoffThreadService;
import org.slj.mqtt.sn.impl.MqttsnMessageQueueProcessor;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

public class MqttsnClientQueueService
        extends AbstractMqttsnBackoffThreadService<IMqttsnClientRuntimeRegistry> implements IMqttsnClientQueueService {

    @Override
    public synchronized void start(IMqttsnClientRuntimeRegistry runtime) throws MqttsnException {
        super.start(runtime);
    }

    @Override
    protected boolean doWork() {
        try {
            Iterator<IMqttsnContext> itr = getRegistry().getMessageQueue().listContexts();
            while(itr.hasNext()){
                IMqttsnContext context = itr.next();
                registry.getQueueProcessor().process(context);
            }
            return true;
        } catch(Exception e){
            logger.log(Level.SEVERE, "error in queue processor thread", e);
            return false;
        }
    }

    protected int getBackoffFactor(){
        return 10;
    }
}
