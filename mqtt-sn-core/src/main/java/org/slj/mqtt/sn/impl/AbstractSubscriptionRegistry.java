package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.Subscription;
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.IMqttsnSubscriptionRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.MqttsnService;
import org.slj.mqtt.sn.utils.TopicPath;

import java.util.Iterator;
import java.util.Set;

public abstract class AbstractSubscriptionRegistry <T extends IMqttsnRuntimeRegistry>
        extends MqttsnService<T>
        implements IMqttsnSubscriptionRegistry<T> {

    @Override
    public boolean subscribe(IMqttsnContext context, String topicPath, int QoS) throws MqttsnException {
        TopicPath path = new TopicPath(topicPath);
        return addSubscription(context, new Subscription(path, QoS));
    }

    @Override
    public boolean unsubscribe(IMqttsnContext context, String topicPath) throws MqttsnException {
        Set<Subscription> paths = readSubscriptions(context);
        TopicPath path = new TopicPath(topicPath);
        return paths.remove(new Subscription(path));
    }

    @Override
    public int getQos(IMqttsnContext context, String topicPath) throws MqttsnException {
        Set<Subscription> paths = readSubscriptions(context);
        if(paths != null && !paths.isEmpty()) {
            Iterator<Subscription> pathItr = paths.iterator();
            client:
            while (pathItr.hasNext()) {
                try {
                    Subscription sub = pathItr.next();
                    TopicPath path = sub.getTopicPath();
                    if (path.matches(topicPath)) {
                        return sub.getQoS();
                    }
                } catch (Exception e) {
                    throw new MqttsnException(e);
                }
            }
        }
        throw new MqttsnException("no matching subscription found for client");
    }

    protected abstract Set<Subscription> readSubscriptions(IMqttsnContext context) throws MqttsnException ;

    protected abstract boolean addSubscription(IMqttsnContext context, Subscription subscription) throws MqttsnException ;
}
