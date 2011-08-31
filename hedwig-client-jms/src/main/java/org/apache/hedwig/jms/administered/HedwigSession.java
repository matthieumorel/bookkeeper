package org.apache.hedwig.jms.administered;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
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

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.HedwigMessageConsumer;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.message.HedwigJMSTextMessage;
import org.apache.hedwig.jms.util.SessionMessageQueue;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;

import com.google.protobuf.ByteString;

public class HedwigSession implements Session {

	protected HedwigConnection connection;
	SessionMessageQueue sessionMessageQueue;
	SessionControlThread sessionControlThread;
	Map<ByteString, MessageListener> listeners = new HashMap<ByteString, MessageListener>();
	Map<ByteString, HedwigMessageConsumer> consumers = new HashMap<ByteString, HedwigMessageConsumer>();
	int ackMode;
	private HedwigClient hedwigClient;
	volatile boolean isClosed = false;

	public HedwigSession(HedwigConnection connection, int ackMode, ClientConfiguration clientConf) {
		this.connection = connection;
		this.ackMode = ackMode;
		this.sessionMessageQueue = new SessionMessageQueue(this);
		this.sessionControlThread = new SessionControlThread(sessionMessageQueue, this);
		// use 1 client per session for sending events
		this.hedwigClient = new HedwigClient(clientConf);

		connection.registerSession(this);

	}

	public void start() throws JMSException {
		Iterator<Entry<ByteString, HedwigMessageConsumer>> iterator = consumers.entrySet().iterator();
		while (iterator.hasNext()) {
			HedwigMessageConsumer consumer = iterator.next().getValue();
			consumer.start();
		}
	}

	public void stop() throws JMSException {
		Iterator<Entry<ByteString, HedwigMessageConsumer>> iterator = consumers.entrySet().iterator();
		while (iterator.hasNext()) {
			HedwigMessageConsumer consumer = iterator.next().getValue();
			consumer.stop();
		}
	}

	public boolean isClosed() {
		return isClosed;
	}

	private void checkSessionNotClosed() throws IllegalStateException {
		if (isClosed) {
			throw new IllegalStateException("Session is closed");
		}
	}

	public HedwigClient getHedwigProducerForSession() {
		return hedwigClient;
	}

	public void send(Destination destination, HedwigJMSMessage message) throws JMSException {
		checkSessionNotClosed();
		sessionMessageQueue.offerMessageToSend(destination, message);
	}

	public void acknowledged(MessageSeqId messageId) throws JMSException {
		sessionMessageQueue.messageAcknowledged(messageId);
	}

	public void messageSuccessfullyDelivered(HedwigJMSMessage message) throws JMSException {
		// do acknowledgement
		switch (ackMode) {
		case Session.AUTO_ACKNOWLEDGE:
			consumers.get(message.getSubscriberId()).acknowledge(message.getMessage().getMsgId());
			break;
		case Session.CLIENT_ACKNOWLEDGE:
			// let the client acknowledge explicitely
			sessionMessageQueue.unacknowledgedMessageDelivered(message);
			break;
		case Session.DUPS_OK_ACKNOWLEDGE:
			// FIXME not sure how to "lazily" acknowledge. Let's simply
			// acknowledge for now
			consumers.get(message.getSubscriberId()).acknowledge(message.getMessage().getMsgId());
			break;
		case Session.SESSION_TRANSACTED:
			// ack comes with a "commit" statement
			sessionMessageQueue.unacknowledgedMessageDelivered(message);
			break;
		default:
			throw new JMSException("There cannot be no acknowledgement mode for a session");
		}
	}

	public int getACKMode() {
		return ackMode;
	}

	public HedwigConnection getHedwigConnection() {
		return connection;
	}

	public void addConsumer(ByteString subscriberId, HedwigMessageConsumer consumer) {

		consumers.put(subscriberId, consumer);
		sessionMessageQueue.addConsumer(subscriberId);
	}

	public HedwigMessageConsumer getConsumer(ByteString subscriberId) {
		return consumers.get(subscriberId);
	}

	public void addListener(ByteString subscriberId, MessageListener listener) {
		if (!sessionControlThread.isAlive()) {
			sessionControlThread.start();
		}
		listeners.put(subscriberId, listener);
	}

	public Map<ByteString, MessageListener> getListeners() {
		return listeners;
	}

	public boolean offerReceivedMessage(org.apache.hedwig.protocol.PubSubProtocol.Message message,
	        ByteString topicName, ByteString subscriberId) {
		return sessionMessageQueue.offerReceivedMessage(subscriberId, topicName, message);
	}

	public HedwigJMSMessage takeNextMessage(ByteString subscriberId) {
		return sessionMessageQueue.retrieve(subscriberId, true, 0, null);
	}

	public HedwigJMSMessage pollNextMessage(ByteString subscriberId, long time, TimeUnit timeUnit) {
		return sessionMessageQueue.retrieve(subscriberId, false, time, timeUnit);
	}

	@Override
	public synchronized void close() throws JMSException {
		if (!isClosed) {
			// stop async delivery of received messages
			sessionControlThread.interrupt();

			if (Session.SESSION_TRANSACTED == ackMode) {
				// TODO check what this means here: should we simply stop the
				// connection, or should we start reception through recovery?
				rollback();
			}
			// disconnect hedwig clients
			for (HedwigMessageConsumer consumer : consumers.values()) {
				consumer.getHedwigClient().stop();
			}
			sessionMessageQueue.clearPendingMessages();
			isClosed = true;
		}

	}

	@Override
	public void commit() throws JMSException {
		checkSessionNotClosed();
		if (!getTransacted()) {
			throw new IllegalStateException("Cannot commit a non-transacted session");
		}
		sessionMessageQueue.commit();
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0, String arg1) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BytesMessage createBytesMessage() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
	        throws JMSException {
		checkSessionNotClosed();
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

		HedwigMessageConsumer consumer = new HedwigMessageConsumer(this, topicName, getHedwigConnection()
		        .getHedwigClientConfig());
		sessionMessageQueue.addConsumer(consumer.getSubscriberId());
		return consumer;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1, String arg2, boolean arg3)
	        throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapMessage createMapMessage() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Message createMessage() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMessage createObjectMessage(Serializable arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageProducer createProducer(Destination arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Queue createQueue(String arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StreamMessage createStreamMessage() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryQueue createTemporaryQueue() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TemporaryTopic createTemporaryTopic() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TextMessage createTextMessage() throws JMSException {
		return new HedwigJMSTextMessage(this);
	}

	@Override
	public TextMessage createTextMessage(String text) throws JMSException {
		checkSessionNotClosed();
		TextMessage textMessage = createTextMessage();
		textMessage.setText(text);
		return textMessage;
	}

	@Override
	public Topic createTopic(String arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getAcknowledgeMode() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getTransacted() throws JMSException {
		checkSessionNotClosed();
		return Session.SESSION_TRANSACTED == ackMode;
	}

	@Override
	public void recover() throws JMSException {
		checkSessionNotClosed();
		// 1. stop message delivery
		Iterator<HedwigMessageConsumer> iterator = consumers.values().iterator();
		while (iterator.hasNext()) {
			HedwigMessageConsumer next = iterator.next();
			try {
				next.getHedwigClient().getSubscriber()
				        .closeSubscription(next.getHedwigTopicName(), next.getSubscriberId());
			} catch (ServiceDownException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " because broker is down", ServiceDownException.class.getName() + "/" + e.getMessage());
			}
		}

		// TODO stop message senders as well

		// 2. mark delivered but unacknowledged messages as "redelivered"
		// we just need to flag the session message queue as "recovering"
		// and incoming messages already processed will be marked as redelivered
		sessionMessageQueue.recoveryInitiated();

		// 3. redeliver
		iterator = consumers.values().iterator();
		while (iterator.hasNext()) {
			HedwigMessageConsumer next = iterator.next();
			try {
				next.getHedwigClient().getSubscriber()
				        .subscribe(next.getHedwigTopicName(), next.getSubscriberId(), CreateOrAttach.CREATE_OR_ATTACH);
			} catch (CouldNotConnectException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " because connection to broker is impossible", CouldNotConnectException.class.getName() + "/"
				        + e.getMessage());
			} catch (ClientAlreadySubscribedException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " due to an internal error ", CouldNotConnectException.class.getName() + "/" + e.getMessage());
			} catch (ServiceDownException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " because broker is down", ServiceDownException.class.getName() + "/" + e.getMessage());
			} catch (InvalidSubscriberIdException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " due to an internal error", InvalidSubscriberIdException.class.getName() + "/"
				        + e.getMessage());
			}
			try {
				next.getHedwigClient().getSubscriber()
				        .startDelivery(next.getHedwigTopicName(), next.getSubscriberId(), next);
			} catch (ClientNotSubscribedException e) {
				throw new JMSException("Cannot " + (getTransacted() ? "rollback" : "recover")
				        + " due to an internal error", ClientNotSubscribedException.class.getName() + "/"
				        + e.getMessage());
			}
		}

	}

	@Override
	public void rollback() throws JMSException {
		checkSessionNotClosed();
		sessionMessageQueue.clearPendingMessages();
		recover();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setMessageListener(MessageListener arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe(String arg0) throws JMSException {
		checkSessionNotClosed();
		// TODO Auto-generated method stub

	}

}