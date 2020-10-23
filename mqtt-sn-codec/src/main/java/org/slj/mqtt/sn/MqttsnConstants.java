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

package org.slj.mqtt.sn;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface MqttsnConstants {

    String ENCODING = "UTF-8";
    Charset CHARSET = StandardCharsets.UTF_8;

    int USIGNED_MAX_16 = 65535;
    int USIGNED_MAX_8 = 255;

    byte TOPIC_NORMAL = 0b00,
            TOPIC_PREDEFINED = 0b01,
            TOPIC_SHORT = 0b10,
            TOPIC_RESERVED = 0b11;

    enum TOPIC_TYPE {

        NORMAL(TOPIC_NORMAL),
        PREDEFINED(TOPIC_PREDEFINED),
        SHORT(TOPIC_SHORT),
        RESERVED(TOPIC_RESERVED);

        byte flag;

        TOPIC_TYPE(byte flag) {
            this.flag = flag;
        }

        public byte getFlag() {
            return flag;
        }
    }

    int RETURN_CODE_ACCEPTED = 0x00,
            RETURN_CODE_REJECTED_CONGESTION = 0x01,
            RETURN_CODE_INVALID_TOPIC_ID = 0x02,
            RETURN_CODE_SERVER_UNAVAILABLE = 0x03;

    int QoS0 = 0,
            QoS1 = 1,
            QoS2 = 2,
            QoSM1 = -1;

    byte ADVERTISE = 0x00;
    byte SEARCHGW = 0x01;
    byte GWINFO = 0x02;
    byte CONNECT = 0x04;
    byte CONNACK = 0x05;
    byte WILLTOPICREQ = 0x06;
    byte WILLTOPIC = 0x07;
    byte WILLMSGREQ = 0x08;
    byte WILLMSG = 0x09;
    byte REGISTER = 0x0A;
    byte REGACK = 0x0B;
    byte PUBLISH = 0x0C;
    byte PUBACK = 0x0D;
    byte PUBCOMP = 0x0E;
    byte PUBREC = 0x0F;
    byte PUBREL = 0x10;
    byte SUBSCRIBE = 0x12;
    byte SUBACK = 0x13;
    byte UNSUBSCRIBE = 0x14;
    byte UNSUBACK = 0x15;
    byte PINGREQ = 0x16;
    byte PINGRESP = 0x17;
    byte DISCONNECT = 0x18;
    byte WILLTOPICUPD = 0x1A;
    byte WILLTOPICRESP = 0x1B;
    byte WILLMSGUPD = 0x1C;
    byte WILLMSGRESP = 0x1D;
    int ENCAPSMSG = 0xFE;

}
