package org.apache.hedwig.jms.administered;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.HedwigMessageConsumer;
import org.apache.hedwig.jms.message.HedwigJMSTextMessage;
import org.apache.hedwig.jms.util.ClientIdGenerator;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;

import com.google.protobuf.ByteString;

public class HedwigSession implements Session {

	protected HedwigConnection connection;

	public HedwigSession(HedwigConnection connection) {
		this.connection = connection;
	}
	
	public HedwigConnection getHedwigConnection() {
		return connection;
	}

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public QueueBrowser createBrowser(Queue arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0, String arg1) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BytesMessage createBytesMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
	        throws JMSException {
		if (noLocal) {
			throw new UnsupportedOperationException(
			        "Hedwig currently cannot inhibit delivery of messages published by the same connection");
		}
		if (messageSelector != null) {
			throw new UnsupportedOperationException("Hedwig currently does not provide message selectors");
		}
		if (!(destination instanceof Topic)) {
			throw new UnsupportedOperationException("Hedwig currently only implements topic subscribers");
		}
			ByteString topicName = ByteString.copyFromUtf8(((Topic) destination).getTopicName());
			return new HedwigMessageConsumer(this, topicName);
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1, String arg2, boolean arg3)
	        throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapMessage createMapMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message createMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage(Serializable arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageProducer createProducer(Destination arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Queue createQueue(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamMessage createStreamMessage() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryQueue createTemporaryQueue() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryTopic createTemporaryTopic() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TextMessage createTextMessage() throws JMSException {
		return new HedwigJMSTextMessage();
	}

	@Override
	public TextMessage createTextMessage(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Topic createTopic(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getAcknowledgeMode() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getTransacted() throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void recover() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setMessageListener(MessageListener arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe(String arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

}
