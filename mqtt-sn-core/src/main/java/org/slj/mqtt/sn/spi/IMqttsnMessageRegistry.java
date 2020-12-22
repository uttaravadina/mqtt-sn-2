package org.slj.mqtt.sn.spi;

import java.util.Date;
import java.util.UUID;

public interface IMqttsnMessageRegistry <T extends IMqttsnRuntimeRegistry> extends IMqttsnRegistry<T>{

    void tidy() throws MqttsnException ;

    UUID add(byte[] data, boolean removeAfterRead) throws MqttsnException ;

    UUID add(byte[] data, Date expires) throws MqttsnException;

    byte[] get(UUID messageId) throws MqttsnException;
}
