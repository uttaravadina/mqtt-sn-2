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

package org.slj.mqtt.sn.wire;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.MqttsnCodecException;

public class MqttsnWireUtils {

    public static void validate8Bit(int field) throws MqttsnCodecException {
        if (field < 0 || field > MqttsnConstants.USIGNED_MAX_8) {
            throw new MqttsnCodecException("invalid unsigned 8 bit number - " + field);
        }
    }

    public static void validate16Bit(int field) throws MqttsnCodecException {
        if (field < 0 || field > MqttsnConstants.USIGNED_MAX_16) {
            throw new MqttsnCodecException("invalid unsigned 16 bit number - " + field);
        }
    }

    public static void validateQoS(int QoS) throws MqttsnCodecException {
        if (QoS != MqttsnConstants.QoSM1 &&
                QoS != MqttsnConstants.QoS0 &&
                QoS != MqttsnConstants.QoS1 &&
                QoS != MqttsnConstants.QoS2) {
            throw new MqttsnCodecException("invalid QoS number - " + QoS);
        }
    }

    public static int readMessageType(byte[] data) {
        int msgType;
        if (MqttsnWireUtils.isExtendedMessage(data)) {
            msgType = (data[3] & 0xFF);
        } else {
            msgType = (data[1] & 0xFF);
        }
        return msgType;
    }

    public static byte readHeaderByteWithOffset(byte[] data, int index) {
        return isExtendedMessage(data) ? data[index + 2] : data[index];
    }

    public static int read8bit(byte b1) {
        return (b1 & 0xFF);
    }

    public static int read16bit(byte b1, byte b2) {
        return ((b1 & 0xFF) << 8) + (b2 & 0xFF);
    }

    public static boolean isExtendedMessage(byte[] data) {
        return data[0] == 0x01;
    }

    public static int readVariableMessageLength(byte[] data) {
        int length = 0;
        if (isExtendedMessage(data)) {
            //big payload
            length = ((data[1] & 0xFF) << 8) + (data[2] & 0xFF);
        } else {
            //small payload
            length = (data[0] & 0xFF);
        }
        return length;
    }

    public static String toBinary(byte... b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; b != null && i < b.length; i++) {
            sb.append(String.format("%8s", Integer.toBinaryString(b[i] & 0xFF)).replace(' ', '0'));
            if (i < b.length - 1)
                sb.append(" ");
        }
        return sb.toString();
    }
}