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

package org.slj.mqtt.sn.client.spi;

import org.slj.mqtt.sn.spi.IMqttsnPublishReceivedListener;
import org.slj.mqtt.sn.spi.IMqttsnPublishSentListener;
import org.slj.mqtt.sn.spi.MqttsnException;

import java.io.Closeable;

/**
 * An SN client allows you to talk to a DISCOVERED or PRECONFIGURED Sensor Network gateway.
 */
public interface IMqttsnClient extends Closeable {

    /**
     * A blocking call to issue a CONNECT packet. On return your client will be considered in ACTIVE mode unless an exception
     * is thrown.
     *
     * @param keepAlive - Time in seconds to keep the session alive before the gateway times you out
     * @param cleanSession - Whether tidy up any existing session state on the gateway; including message queues, subscriptions and registrations
     * @throws MqttsnException - There was an error processing the CONNECT command
     */
    void connect(int keepAlive, boolean cleanSession) throws MqttsnException;

    /**
     * Add a new message onto the queue to send to the gateway at some point in the future.
     * The queue is processed in FIFO order in the background on a queue processing thread.
     * You can be notified of successful completion by registering a messageSent listener
     * onto the client.
     *
     * @param topicName - The path to which you wish to send the data
     * @param QoS - Quality of Service of the method, one of -1, 0 , 1, 2
     * @param data - The data you wish to send
     * @throws MqttsnException
     */
    void publish(String topicName, int QoS, byte[] data) throws MqttsnException;


    /**
     * Subscribe the topic using the most appropriate topic scheme. This will automatically select th use of a PREDEFINED or SHORT
     * according to your configuration. If no PREDEFINED or SHORT topic is available, a new registration will be places in your
     * topic registry transparently for receiving the messages.
     *
     * @param topicName - The path to subscribe to
     * @param QoS - The quality of service of the subscription
     * @throws MqttsnException
     */
    void subscribe(String topicName, int QoS) throws MqttsnException;

    /**
     * Unsubscribe the topic using the most appropriate topic scheme. This will automatically select th use of a PREDEFINED or SHORT
     * according to your configuration. If no PREDEFINED or SHORT topic is available, a new registration will be places in your
     * topic registry transparently for receiving the messages.
     *
     * @param topicName - The path to unsubscribe to
     * @throws MqttsnException
     */
    void unsubscribe(String topicName) throws MqttsnException;

    /**
     * A long blocking call which will put your client into the SLEEP state for the {duration} specified in SECONDS, automatically
     * waking every {wakeAfterInterval} period of time in SECONDS to check for messages. NOTE: messages queued to be sent while
     * the device is SLEEPing will not be processed until the device is back in the ACTIVE status.
     *
     * @param duration - time in seconds for the sleep period to last
     * @param wakeAfterInterval - time in seconds that the device will wake up to check for messages, before going back to sleep
     * @param maxWaitTime - time during WAKING that the device will wait for a PINGRESP response from the gateway before erroring
     * @param connectOnFinish - when the DURATION period has elapsed, should the device transition into the ACTIVE mode by issuing a soft CONNECT or DISCONNECT
     * @throws MqttsnException
     */
    void supervisedSleepWithWake(int duration, int wakeAfterInterval, int maxWaitTime, boolean connectOnFinish)  throws MqttsnException;

    /**
     * Put the device into the SLEEP mode for the duration in seconds. NOTE: this is a non-supervized sleep, which means the application
     * is resonsible for issuing PING and CONNECTS from this mode
     * @param duration - Time in seconds to put the device to sleep.
     * @throws MqttsnException
     */
    void sleep(int duration) throws MqttsnException;

    /**
     * Unsupervised Wake the device up by issuing a PINGREQ from SLEEP state. The maxWait time will be taken from the core client configuration
     * supplied during setup.
     * @throws MqttsnException
     */
    void wake()  throws MqttsnException;

    /**
     * Unsupervised Wake the device up by issuing a PINGREQ from SLEEP state.
     * @param waitTime - Time in MILLISECONDS to wait for a PINGRESP after the AWAKE period
     * @throws MqttsnException
     */
    void wake(int waitTime)  throws MqttsnException;

    /**
     * DISCONNECT from the gateway. Close down any local queues.
     */
    void disconnect() throws MqttsnException;

    /**
     * Registers a new Publish listener which will be notified when a PUBLISH message is successfully committed to the gateway
     * @param listener - The instance of the listener to notify
     */
    void registerSentListener(IMqttsnPublishSentListener listener);

    /**
     * Registers a new Publish listener which will be notified when a PUBLISH message is successfully RECEIVED committed from the gateway
     * @param listener - The instance of the listener to notify
     */
    void registerReceivedListener(IMqttsnPublishReceivedListener listener);

    /**
     * Return the clientId associated with this instance. The clientId is passed to the client from the configuration (contextId).
     * @return - Return the clientId associated with this instance
     */
    String getClientId();

}