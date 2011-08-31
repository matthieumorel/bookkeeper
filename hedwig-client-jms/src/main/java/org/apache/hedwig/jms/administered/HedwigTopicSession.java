package org.apache.hedwig.jms.administered;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.jms.HedwigTopicPublisher;
import org.apache.hedwig.jms.HedwigTopicSubscriber;

import com.google.protobuf.ByteString;

public class HedwigTopicSession extends HedwigSession implements TopicSession {

	public HedwigTopicSession(HedwigConnection connection, int ackMode, ClientConfiguration clientConfig) {
		super(connection, ackMode, clientConfig);
	}

	@Override
	public TopicPublisher createPublisher(Topic topic) throws JMSException {
		return new HedwigTopicPublisher(this, topic);
	}

	@Override
	public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
		return createSubscriber(topic, null, false);
	}

	@Override
	public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
		if (messageSelector != null) {
			throw new UnsupportedOperationException("Hedwig currently does not provide message selectors");
		}
		if (noLocal) {
			throw new UnsupportedOperationException(
			        "Hedwig currently does not distinguish between local and non local subscriptions");
		}
		return new HedwigTopicSubscriber(this, ByteString.copyFromUtf8(topic.getTopicName()), getHedwigConnection()
		        .getHedwigClientConfig());
	}

}
