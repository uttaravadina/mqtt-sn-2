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

package org.slj.mqtt.sn.wire.version1_2;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.codec.AbstractMqttsnCodec;
import org.slj.mqtt.sn.codec.MqttsnCodecException;
import org.slj.mqtt.sn.spi.IMqttsnMessageFactory;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;
import org.slj.mqtt.sn.wire.version1_2.payload.*;

public class Mqttsn_v1_2_Codec extends AbstractMqttsnCodec {

    protected IMqttsnMessageFactory messageFactory;

    protected AbstractMqttsnMessage createInstance(byte[] data) throws MqttsnCodecException {

        if (data == null || data.length < 2)
            throw new MqttsnCodecException("malformed mqtt-sn packet");

        AbstractMqttsnMessage msg = null;
        int msgType = MqttsnWireUtils.readMessageType(data);

        switch (msgType) {
            case MqttsnConstants.ADVERTISE:
                validateLengthGreaterThanOrEquals(data, 3);
                msg = new MqttsnAdvertise();
                break;
            case MqttsnConstants.SEARCHGW:
                validateLengthEquals(data, 3);
                msg = new MqttsnSearchGw();
                break;
            case MqttsnConstants.GWINFO:
                validateLengthGreaterThanOrEquals(data, 3);
                msg = new MqttsnGwInfo();
                break;
            case MqttsnConstants.CONNECT:
                validateLengthGreaterThanOrEquals(data, 6);
                msg = new MqttsnConnect();
                break;
            case MqttsnConstants.CONNACK:
                validateLengthEquals(data, 3);
                msg = new MqttsnConnack();
                break;
            case MqttsnConstants.REGISTER:
                validateLengthGreaterThanOrEquals(data, 7);
                msg = new MqttsnRegister();
                break;
            case MqttsnConstants.REGACK:
                validateLengthEquals(data, 7);
                msg = new MqttsnRegack();
                break;
            case MqttsnConstants.PUBLISH:
                validateLengthGreaterThanOrEquals(data, 7);
                msg = new MqttsnPublish();
                msg.decode(data);
                break;
            case MqttsnConstants.PUBACK:
                validateLengthEquals(data, 7);
                msg = new MqttsnPuback();
                break;
            case MqttsnConstants.PUBCOMP:
                validateLengthEquals(data, 4);
                msg = new MqttsnPubcomp();
                break;
            case MqttsnConstants.PUBREC:
                validateLengthEquals(data, 4);
                msg = new MqttsnPubrec();
                break;
            case MqttsnConstants.PUBREL:
                validateLengthEquals(data, 4);
                msg = new MqttsnPubrel();
                break;
            case MqttsnConstants.PINGREQ:
                validateLengthGreaterThanOrEquals(data, 2);
                msg = new MqttsnPingreq();
                break;
            case MqttsnConstants.PINGRESP:
                validateLengthEquals(data, 2);
                msg = new MqttsnPingresp();
                break;
            case MqttsnConstants.DISCONNECT:
                validateLengthGreaterThanOrEquals(data, 2);
                msg = new MqttsnDisconnect();
                break;
            case MqttsnConstants.SUBSCRIBE:
                validateLengthGreaterThanOrEquals(data, 7);
                msg = new MqttsnSubscribe();
                break;
            case MqttsnConstants.SUBACK:
                validateLengthEquals(data, 8);
                msg = new MqttsnSuback();
                break;
            case MqttsnConstants.UNSUBSCRIBE:
                validateLengthGreaterThanOrEquals(data, 7);
                msg = new MqttsnUnsubscribe();
                break;
            case MqttsnConstants.UNSUBACK:
                validateLengthEquals(data, 4);
                msg = new MqttsnUnsuback();
                break;
            case MqttsnConstants.WILLTOPICREQ:
                validateLengthEquals(data, 2);
                msg = new MqttsnWilltopicreq();
                break;
            case MqttsnConstants.WILLTOPIC:
                validateLengthGreaterThanOrEquals(data, 3);
                msg = new MqttsnWilltopic();
                break;
            case MqttsnConstants.WILLMSGREQ:
                validateLengthEquals(data, 2);
                msg = new MqttsnWillmsgreq();
                break;
            case MqttsnConstants.WILLMSG:
                validateLengthGreaterThanOrEquals(data, 2);
                msg = new MqttsnWillmsg();
                break;
            case MqttsnConstants.WILLTOPICUPD:
                validateLengthGreaterThanOrEquals(data, 3);
                msg = new MqttsnWilltopicudp();
                break;
            case MqttsnConstants.WILLTOPICRESP:
                validateLengthEquals(data, 3);
                msg = new MqttsnWilltopicresp();
                break;
            case MqttsnConstants.WILLMSGUPD:
                validateLengthGreaterThanOrEquals(data, 2);
                msg = new MqttsnWillmsgupd();
                break;
            case MqttsnConstants.WILLMSGRESP:
                validateLengthEquals(data, 3);
                msg = new MqttsnWillmsgresp();
                break;
            case MqttsnConstants.ENCAPSMSG:
                validateLengthGreaterThanOrEquals(data, 5);
                msg = new MqttsnEncapsmsg();
                break;
            default:
                throw new MqttsnCodecException(String.format("unknown message type [%s]", msgType));
        }
        return msg;
    }

    @Override
    public IMqttsnMessageFactory createMessageFactory() {
        if (messageFactory == null) {
            synchronized (this) {
                if (messageFactory == null) messageFactory = Mqttsn_v1_2_MessageFactory.getInstance();
            }
        }
        return messageFactory;
    }
}