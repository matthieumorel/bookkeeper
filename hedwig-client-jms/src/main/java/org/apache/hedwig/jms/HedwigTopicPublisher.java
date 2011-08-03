package org.apache.hedwig.jms;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.util.JMSUtils;
import org.apache.hedwig.util.Callback;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;

public class HedwigTopicPublisher extends HedwigMessageProducer implements TopicPublisher {

	Topic topic;
	static final int DEFAULT_DELIVERY_MODE = DeliveryMode.PERSISTENT;
	static final int DEFAULT_PRIORITY = 0;
	static final int DEFAULT_TIME_TO_LIVE = 10000;

	public HedwigTopicPublisher(HedwigSession hedwigSession, Topic topic) {
		super(hedwigSession);
		this.topic = topic;
	}

	@Override
	public Topic getTopic() throws JMSException {
		return topic;
	}

	@Override
	public void publish(Message message) throws JMSException {
		publish(this.topic, message, DEFAULT_DELIVERY_MODE, DEFAULT_PRIORITY, DEFAULT_TIME_TO_LIVE);
	}

	@Override
	public void publish(Topic topic, Message message) throws JMSException {
		publish(topic, message, DEFAULT_DELIVERY_MODE, DEFAULT_PRIORITY, DEFAULT_TIME_TO_LIVE);
	}

	@Override
	public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
		publish(this.topic, message, deliveryMode, priority, timeToLive);
	}

	@Override
	public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive)
	        throws JMSException {
		// Note: delivery mode is ignored, since all messages are persistent in
		// Hedwig
		// FIXME message may be lost however in the async comm

		// TODO priorities: there is no such notion in Hedwig. However, the JMS
		// spec asks ("[provider] should do its best")
		// ... to at least deliver high priority messages ahead of low
		// priority ones.

		// TODO ttl: JMS spec says that
		// "it is not acceptable to ignore time-to-live"
		message.setJMSTimestamp(System.currentTimeMillis());
		// JMS: If the time-to-live is specified as zero, expiration is set to
		// zero to indicate that the message does not expire.
		message.setJMSExpiration(timeToLive == 0 ? 0 : message.getJMSTimestamp() + timeToLive);
		try {
	        hedwigSession.getHedwigConnection().getHedwigClient().getPublisher().publish(ByteString.copyFromUtf8(topic.getTopicName()),
	                ((HedwigJMSMessage) message).getHedwigMessage());
        } catch (CouldNotConnectException e) {
	        JMSUtils.createJMSException("Cannot publish message: cannot connect to broker", e);
        } catch (ServiceDownException e) {
	        JMSUtils.createJMSException("Cannot publish message: broker down?", e);
        }

	}

	public class PublisherCallback implements Callback<Void> {

		String topicName;

		public PublisherCallback(String topicName) {
			this.topicName = topicName;
		}

		@Override
		public void operationFinished(Object ctx, Void resultOfOperation) {
			Logger.getLogger(PublisherCallback.class).info("Successfully sent message on topic " + topicName);
		}

		@Override
		public void operationFailed(Object ctx, PubSubException exception) {
			Logger.getLogger(PublisherCallback.class)
			        .error("Failed to send message on topic : " + topicName, exception);
		}

	}

}
