package org.apache.hedwig.jms.administered;

import javax.jms.JMSException;
import javax.jms.Queue;

public class HedwigQueue extends HedwigDestination implements Queue {

    String name;

    public HedwigQueue(String name) {
        this.name = name;
    }

    @Override
    public String getQueueName() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte getDestinationType() {
        return HedwigDestination.QUEUE_DEST_TYPE;
    }

    @Override
    public String[] getDestinationPaths() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isQueue() {
        return true;
    }

    @Override
    public String getPhysicalName() {
        return name;
    }

    @Override
    public boolean isTemporary() {
        return false;
    }

}
