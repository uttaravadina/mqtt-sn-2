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

package org.slj.mqtt.sn.spi;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.MqttsnWaitToken;
import org.slj.mqtt.sn.model.QueuedPublishMessage;

import java.util.Optional;

public interface IMqttsnMessageStateService<T extends IMqttsnRuntimeRegistry> extends IMqttsnRegistry<T> {

    /**
     * Dispatch a new message to the transport layer, binding it to the state service en route for tracking.
     * @param context - the recipient of the message
     * @param message - the wire message to send
     * @param queuedPublishMessage - reference to the queues message who originated this send (when its of type Publish);
     * @throws MqttsnException
     */
    MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message, QueuedPublishMessage queuedPublishMessage) throws MqttsnException;

    /**
     * Dispatch a new message to the transport layer, binding it to the state service en route for tracking.
     * @param context
     * @param message
     * @throws MqttsnException
     */
    MqttsnWaitToken sendMessage(IMqttsnContext context, IMqttsnMessage message) throws MqttsnException;


    /**
     * Notify into the state service that a new message has arrived from the transport layer.
     * @param context - the context from which the message was received
     * @param message - the message received from the transport layer
     * @param evictExistingInflight - where a messages is already inflight, should this message evict the inflight message and replace it (happens when collisions
     *                              occur)
     * @return the messsage (if any) that was confirmed by the receipt of the inbound message
     * @throws MqttsnException
     */
    IMqttsnMessage notifyMessageReceived(IMqttsnContext context, IMqttsnMessage message, boolean evictExistingInflight) throws MqttsnException;

    /**
     * Join the message sent in waiting for the subsequent confirmation if it needs one
     * @param context - The context to whom you are speaking
     * @param message - The message sent for which you are awaiting a reply
     * @return An optional which will contain either the confirmation message associated with the
     * message supplied OR optional NULL where the message does not require a reply
     * @throws MqttsnExpectationFailedException - When no confirmation was recieved in the time period
     * specified by the runtime options
     */
    Optional<IMqttsnMessage> waitForCompletion(IMqttsnContext context, MqttsnWaitToken token) throws MqttsnExpectationFailedException;

    int countInflight(IMqttsnContext context) throws MqttsnException ;
}
