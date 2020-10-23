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

package org.slj.mqtt.sn.wire.version1_2.payload;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;

/**
 * NB: despite the spec only allowing 23 chars in the clientId field, this type has been designed safely to support
 * clientIds which take the message into an extended type (> 255).
 */
public class MqttsnConnect extends AbstractMqttsnMessageWithFlagsField {

    /* The Duration field is 2-octet long and specifies the duration of a time period in seconds.
    The maximum value that can be encoded is approximately 18 hours. */
    protected int duration;

    protected int protocolId;

    /* 1-23 characters long string that uniquely identifies the client to the server */
    protected String clientId = null;

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getProtocolId() {
        return protocolId;
    }

    public void setProtocolId(int protocolId) {
        this.protocolId = protocolId;
    }

    @Override
    public int getMessageType() {
        return MqttsnConstants.CONNECT;
    }

    @Override
    public void decode(byte[] data) throws MqttsnCodecException {

        if (MqttsnWireUtils.isExtendedMessage(data)) {
            readFlags(data[4]);
        } else {
            readFlags(data[2]);
        }

        protocolId = read8BitAdjusted(data, 3);
        duration = read16BitAdjusted(data, 4);

        byte[] body = readRemainingBytesFromIndexAdjusted(data, 6);
        if (body.length > 0) {
            clientId = new String(body, MqttsnConstants.CHARSET);
        }
    }

    @Override
    public byte[] encode() throws MqttsnCodecException {

        int length = 6 + (clientId == null ? 0 : clientId.length());
        byte[] msg = null;
        int idx = 0;
        if ((length) > 0xFF) {
            length += 2;
            msg = new byte[length];
            msg[idx++] = (byte) 0x01;
            msg[idx++] = ((byte) (0xFF & (length >> 8)));
            msg[idx++] = ((byte) (0xFF & length));
        } else {
            msg = new byte[length];
            msg[idx++] = (byte) length;
        }

        msg[idx++] = (byte) getMessageType();
        msg[idx++] = writeFlags();
        msg[idx++] = (byte) protocolId; //protocol id

        msg[idx++] = (byte) ((duration >> 8) & 0xFF);
        msg[idx++] = (byte) (duration & 0xFF);

        if (clientId != null) {
            byte[] clientIdArr = clientId.getBytes(MqttsnConstants.CHARSET);
            System.arraycopy(clientIdArr, 0, msg, idx, clientIdArr.length);
        }

        return msg;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MqttsnConnect{");
        sb.append("duration=").append(duration);
        sb.append(", protocolId=").append(protocolId);
        sb.append(", clientId='").append(clientId).append('\'');
        sb.append(", will=").append(will);
        sb.append(", cleanSession=").append(cleanSession);
        sb.append('}');
        return sb.toString();
    }
}
