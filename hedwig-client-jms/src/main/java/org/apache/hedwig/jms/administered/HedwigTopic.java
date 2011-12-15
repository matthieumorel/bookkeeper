package org.apache.hedwig.jms.administered;

import javax.jms.JMSException;
import javax.jms.Topic;

public class HedwigTopic extends HedwigDestination implements Topic {

    String topicName;

    public HedwigTopic(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    @Override
    public byte getDestinationType() {
        return HedwigDestination.TOPIC_DEST_TYPE;
    }

    @Override
    public String[] getDestinationPaths() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isQueue() {
        return false;
    }

    @Override
    public String getPhysicalName() {
        return topicName;
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

}
