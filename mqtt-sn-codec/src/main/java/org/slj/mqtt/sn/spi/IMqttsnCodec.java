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

import org.slj.mqtt.sn.codec.MqttsnCodecException;

/**
 * A codec contains all the functionality to marshall and unmarshall
 * wire traffic in the format specified by the implementation. Further,
 * it also provides a message factory instance which allows construction
 * of wire messages hiding the unlying transport format. This allows versioned
 * protocol support.
 *
 * @author Simon Johnson <simon622 AT gmail DOT com>
 */
public interface IMqttsnCodec {

    /**
     * To help with debugging, this method will return a binary or hex
     * representation of the encoded message
     */
    String print(IMqttsnMessage message) throws MqttsnCodecException;

    /**
     * Given data of the wire, will convert to the message model which can be used
     * in a given runtime
     *
     * @throws MqttsnCodecException - something went wrong when decoding the data
     */
    IMqttsnMessage decode(byte[] data) throws MqttsnCodecException;

    /**
     * When supplied with messages constructed from an associated message factory,
     * will encode them into data that can be sent on the wire
     *
     * @throws MqttsnCodecException - something went wrong when encoding the data
     */
    byte[] encode(IMqttsnMessage message) throws MqttsnCodecException;

    /**
     * A message factory will contruct messages using convenience methods
     * that hide the complexity of the underlying wire format
     */
    IMqttsnMessageFactory createMessageFactory();
}
