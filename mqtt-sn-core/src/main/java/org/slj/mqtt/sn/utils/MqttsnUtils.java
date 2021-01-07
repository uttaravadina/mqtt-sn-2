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

package org.slj.mqtt.sn.utils;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.model.MqttsnClientState;
import org.slj.mqtt.sn.model.MqttsnWaitToken;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.MqttsnExpectationFailedException;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MqttsnUtils {

    private static Logger logger = Logger.getLogger(MqttsnUtils.class.getName());

    public static byte[] arrayOf(int size, byte fill){
        byte[] a = new byte[size];
        Arrays.fill(a, fill);
        return a;
    }

    public static boolean in(MqttsnClientState state, MqttsnClientState... options){
        if(options == null) return false;
        for (int i = 0; i < options.length; i++) {
            if(options[i] == state) return true;
        }
        return false;
    }

    public static void responseCheck(MqttsnWaitToken token, Optional<IMqttsnMessage> response)
            throws MqttsnExpectationFailedException{
        if(response.isPresent() &&
                response.get().isErrorMessage()){
            logger.log(Level.WARNING, "error response received from gateway, operation failed; throw to application");
            throw new MqttsnExpectationFailedException("error response received from gateway, operation failed");
        }
        if(token.isError()){
            logger.log(Level.WARNING, "token was marked invalid by state machine; throw to application");
            throw new MqttsnExpectationFailedException("token was marked invalid by state machine");
        }
    }

    public static int getNextLeaseId(Collection<Integer> used, int startAt) throws MqttsnException {
        if(used.isEmpty()) return startAt;
        if(used.size() == ((0xFFFF - startAt) + 1)) throw new MqttsnException("all leases taken");
        TreeSet<Integer> sortedIds = new TreeSet<>(used);
        Integer highest = sortedIds.last();
        if(highest >= 0xFFFF)
            throw new MqttsnException("no alias left for use for client");

        int nextValue = highest.intValue();
        do {
            nextValue++;
            if(!used.contains(nextValue)) return nextValue;
        } while(nextValue <= 0xFFFF);
        throw new MqttsnException("unable to assigned lease client");
    }

    public static String getDurationString(long millis) {

        if(millis < 0) {
            throw new IllegalArgumentException("must be greater than zero!");
        }

        if(millis < 1000){
            return String.format("%s millisecond%s", millis, millis > 1 ? "s" : "");
        }

        long days = TimeUnit.MILLISECONDS.toDays(millis);
        millis -= TimeUnit.DAYS.toMillis(days);
        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

        StringBuilder sb = new StringBuilder();

        if(days > 0) {
            sb.append(days);
            sb.append(String.format(" day%s, ", days > 1 ? "s" : ""));
        }

        if(days > 0 || hours > 0) {
            sb.append(hours);
            sb.append(String.format(" hour%s, ", hours > 1 || hours == 0 ? "s" : ""));
        }

        if(hours > 0 || days > 0 || minutes > 0) {
            sb.append(minutes);
            sb.append(String.format(" minute%s, ", minutes > 1 || minutes == 0 ? "s" : ""));
        }

        sb.append(seconds);
        sb.append(String.format(" second%s", seconds > 1 ? "s" : ""));

        return(sb.toString());
    }

    public static <T extends Object> boolean contains(T[] haystack, T needle){
        if(haystack == null || haystack.length == 0) return false;
        for (int i = 0; i < haystack.length; i++) {
            if(Objects.equals(haystack[i], needle)){
                return true;
            }
        }
        return false;
    }

    public static boolean validTopicScheme(int topicIdType, byte[] topicBytes, boolean topicDataAsString) {
        if(topicIdType == MqttsnConstants.TOPIC_PREDEFINED){
            return topicBytes.length == 2;
        } else if(topicIdType == MqttsnConstants.TOPIC_NORMAL){
            return topicDataAsString ? validTopicName(new String(topicBytes, MqttsnConstants.CHARSET)) : topicBytes.length == 2;
        } else if(topicIdType == MqttsnConstants.TOPIC_SHORT){
            return topicBytes.length == 2;
        }
        else
            return false;
    }

    public static boolean validTopicName(String topicString) {
       return topicString != null && topicString.trim().length() > 0;
    }

    public static boolean validQos(int value) {
        if(value < -1 || value > 2)
            return false;
        return true;
    }

    public static boolean validUInt16(int value) {
        if(value < 0 || value > MqttsnConstants.USIGNED_MAX_16)
            return false;
        return true;
    }

    public static boolean validInt8(int value) throws MqttsnExpectationFailedException{
        if(value < 0 || value > MqttsnConstants.USIGNED_MAX_8)
            return false;
        return true;
    }
}
