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

}
