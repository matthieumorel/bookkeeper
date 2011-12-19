package org.apache.hedwig.jms;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.administered.HedwigTopic;

import com.google.protobuf.ByteString;

public class HedwigTopicSubscriber extends HedwigMessageConsumer implements TopicSubscriber {

    public HedwigTopicSubscriber(HedwigSession hedwigSession, ByteString topicName,
            ClientConfiguration hedwigClientConfig, String selector) throws JMSException {
        super(hedwigSession, topicName, hedwigClientConfig, selector);
        // TODO Auto-generated constructor stub
    }

    @Override
    public Topic getTopic() throws JMSException {
        return new HedwigTopic(topicName.toStringUtf8());
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return false;
    }

}
