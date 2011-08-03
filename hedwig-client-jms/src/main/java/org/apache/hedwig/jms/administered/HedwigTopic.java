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

}
