package org.slj.mqtt.sn.impl;

import org.slj.mqtt.sn.MqttsnConstants;
import org.slj.mqtt.sn.model.IMqttsnContext;
import org.slj.mqtt.sn.model.MqttsnContext;
import org.slj.mqtt.sn.model.TopicInfo;
import org.slj.mqtt.sn.spi.*;
import org.slj.mqtt.sn.utils.MqttsnUtils;
import org.slj.mqtt.sn.wire.MqttsnWireUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

public abstract class AbstractTopicRegistry <T extends IMqttsnRuntimeRegistry>
        extends AbstractRationalTopicService<T>
        implements IMqttsnTopicRegistry<T> {

    @Override
    public TopicInfo register(IMqttsnContext context, String topicPath) throws MqttsnException {
        Map<String, Integer> map = getRegistrations(context);
        if(map.size() >= registry.getOptions().getMaxTopicsInRegistry()){
            logger.log(Level.WARNING, String.format("max number of registered topics reached for client [%s] >= [%s]", context, map.size()));
            throw new MqttsnException("max number of registered topics reached for client");
        }
        synchronized (context){
            int alias = MqttsnUtils.getNextLeaseId(map.values(), Math.max(1, registry.getOptions().getAliasStartAt()));
            addOrUpdateRegistration(context, rationalizeTopic(context, topicPath), alias);
            TopicInfo info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL,  alias);
            return info;
        }
    }

    @Override
    public void register(IMqttsnContext context, String topicPath, int topicAlias) throws MqttsnException {
        Map<String, Integer> map = getRegistrations(context);
        if(map.containsKey(topicPath)){
            //update existing
            addOrUpdateRegistration(context, rationalizeTopic(context, topicPath), topicAlias);
        } else {
            if(map.size() >= registry.getOptions().getMaxTopicsInRegistry()){
                logger.log(Level.WARNING, String.format("max number of registered topics reached for client [%s] >= [%s]", context, map.size()));
                throw new MqttsnException("max number of registered topics reached for client");
            }
            addOrUpdateRegistration(context, rationalizeTopic(context, topicPath), topicAlias);
        }
    }

    @Override
    public boolean registered(IMqttsnContext context, String topicPath) throws MqttsnException {
        Map<String, Integer> map = getRegistrations(context);
        return map.containsKey(rationalizeTopic(context, topicPath));
    }

    @Override
    public String topicPath(IMqttsnContext context, TopicInfo topicInfo, boolean considerContext) throws MqttsnException {
        String topicPath = null;
        switch (topicInfo.getType()){
            case SHORT:
                topicPath = topicInfo.getTopicPath();
                break;
            case PREDEFINED:
                topicPath = lookupPredefined(context, topicInfo.getTopicId());
                break;
            case NORMAL:
                if(considerContext){
                    if(context == null) throw new MqttsnExpectationFailedException("<null> context cannot be considered");
                    topicPath = lookupRegistered(context, topicInfo.getTopicId());
                }
                break;
            default:
            case RESERVED:
                break;
        }

        if(topicPath == null) {
            logger.log(Level.WARNING, String.format("unable to find matching topicPath in system for [%s] -> [%s]", topicInfo, context));
            throw new MqttsnExpectationFailedException("unable to find matching topicPath in system");
        }
        return rationalizeTopic(context, topicPath);
    }

    @Override
    public String lookupRegistered(IMqttsnContext context, int topicAlias) throws MqttsnException {
        Map<String, Integer> map = getRegistrations(context);
        Iterator<String> itr = map.keySet().iterator();
        synchronized (context){
            while(itr.hasNext()){
                String topicPath = itr.next();
                Integer i = map.get(topicPath);
                if(i != null && i.intValue() == topicAlias)
                    return rationalizeTopic(context, topicPath);
            }
        }
        return null;
    }

    @Override
    public Integer lookupRegistered(IMqttsnContext context, String topicPath) throws MqttsnException {
        Map<String, Integer> map = getRegistrations(context);
        return map.get(rationalizeTopic(context, topicPath));
    }

    @Override
    public Integer lookupPredefined(IMqttsnContext context, String topicPath) throws MqttsnException {
        Map<String, Integer> predefinedMap = getPredefinedTopics(context);
        return predefinedMap.get(rationalizeTopic(context, topicPath));
    }

    @Override
    public String lookupPredefined(IMqttsnContext context, int topicAlias) throws MqttsnException {
        Map<String, Integer> predefinedMap = getPredefinedTopics(context);
        Iterator<String> itr = predefinedMap.keySet().iterator();
        synchronized (predefinedMap){
            while(itr.hasNext()){
                String topicPath = itr.next();
                Integer i = predefinedMap.get(topicPath);
                if(i != null && i.intValue() == topicAlias)
                    return rationalizeTopic(context, topicPath);
            }
        }
        return null;
    }

    @Override
    public TopicInfo lookup(IMqttsnContext context, String topicPath) throws MqttsnException {

        topicPath = rationalizeTopic(context, topicPath);

        //-- check normal first
        TopicInfo info = null;
        if(registered(context, topicPath)){
            Integer topicAlias = lookupRegistered(context, topicPath);
            if(topicAlias != null){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, topicAlias);
            }
        }

        //-- check predefined if nothing in session registry
        if(info == null){
            Integer topicAlias = lookupPredefined(context, topicPath);
            if(topicAlias != null){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.PREDEFINED, topicAlias);
            }
        }

        //-- if topicPath < 2 chars
        if(info == null){
            if(topicPath.length() <= 2){
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.SHORT, topicPath);
            }
        }

        logger.log(Level.INFO, String.format("topic-registry lookup for [%s] => [%s] found [%s]", context, topicPath, info));
        return info;
    }

    @Override
    public TopicInfo normalize(byte topicIdType, byte[] topicData, boolean normalAsLong) throws MqttsnException {
        TopicInfo info = null;
        switch (topicIdType){
            case MqttsnConstants.TOPIC_SHORT:
                if(topicData.length != 2){
                    throw new MqttsnExpectationFailedException("short topics must be exactly 2 bytes");
                }
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.SHORT, new String(topicData, MqttsnConstants.CHARSET));
                break;
            case MqttsnConstants.TOPIC_NORMAL:
                if(normalAsLong){ //-- in the case of a subscribe, the normal actually means the full topic name
                    info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, new String(topicData, MqttsnConstants.CHARSET));
                } else {
                    if(topicData.length != 2){
                        throw new MqttsnExpectationFailedException("normal topic aliases must be exactly 2 bytes");
                    }
                    info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.NORMAL, MqttsnWireUtils.read16bit(topicData[0],topicData[1]));
                }
                break;
            case MqttsnConstants.TOPIC_PREDEFINED:
                if(topicData.length != 2){
                    throw new MqttsnExpectationFailedException("predefined topic aliases must be exactly 2 bytes");
                }
                info = new TopicInfo(MqttsnConstants.TOPIC_TYPE.PREDEFINED, MqttsnWireUtils.read16bit(topicData[0],topicData[1]));
                break;
        }
        return info;
    }

    protected abstract boolean addOrUpdateRegistration(IMqttsnContext context, String topicPath, int alias) throws MqttsnException;

    protected abstract Map<String, Integer> getRegistrations(IMqttsnContext context) throws MqttsnException;

    protected abstract Map<String, Integer> getPredefinedTopics(IMqttsnContext context) throws MqttsnException;
}
