package org.apache.hedwig.jms;

import java.net.MalformedURLException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Assert;
import org.junit.Test;

public class TestACK extends HedwigJMSBaseTest {

	private static final int MAX_MESSAGES = 1000;
	private static final int CLIENT_ACK_UNTIL = 42;
	private static final int COMMIT_INDEX = 42;

	public void testTransactedSession() throws Exception {

	}

	@Test
	public void testAutoACK() throws Exception {
		testAckMode(Session.AUTO_ACKNOWLEDGE);
	}

	@Test
	public void testDupsOKACK() throws Exception {
		// same as auto mode for hedwig-based implementation (for now at least,
		// since there is no "lazy" acknowledgement)
		testAckMode(Session.DUPS_OK_ACKNOWLEDGE);
	}

	private void testAckMode(int ackMode) throws ConfigurationException, MalformedURLException, Exception,
	        NamingException, JMSException, InterruptedException {

		System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();

		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnectionPublisher = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSessionPublisher = topicConnectionPublisher.createTopicSession(false,
		        Session.AUTO_ACKNOWLEDGE);

		TopicConnectionFactory topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		TopicConnection topicConnectionSubscriber = topicConnectionFactorySubscriber.createTopicConnection();
		TopicSession topicSessionSubscriber = topicConnectionSubscriber.createTopicSession(false, ackMode);
		final TopicSubscriber subscriber = topicSessionSubscriber.createSubscriber(topic);
		// since the subscriber only receives
		// messages published *after* the subscription operation, we must
		// create the subscriber now
		Thread.sleep(4000);

		TopicPublisher topicPublisher = topicSessionPublisher.createPublisher(topic);
		for (int i = 0; i < MAX_MESSAGES; i++) {
			TextMessage message = topicSessionPublisher.createTextMessage();
			message.setText("message #" + i);
			topicPublisher.publish(message);
		}

		topicConnectionSubscriber.start();
		int i;
		for (i = 0; i < MAX_MESSAGES; i++) {
			Message received = subscriber.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
		}

		// FIXME without following sleep, hedwig just gets nuts when stopping.
		// Need to figure what's exactly happening
		Thread.sleep(1000);
		topicSessionSubscriber.recover();

		// for auto acknowledge, messages should all be already acknowledged. So
		// we should get no message at all
		Message received = subscriber.receive(5000);
		Assert.assertNull(received);
	}

	@Test
	public void testClientACK() throws Exception {

		System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();

		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

		TopicConnectionFactory topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		TopicConnection topicConnectionSubscriber = topicConnectionFactorySubscriber.createTopicConnection();
		TopicSession topicSessionSubscriber = topicConnectionSubscriber.createTopicSession(false,
		        Session.CLIENT_ACKNOWLEDGE);
		final TopicSubscriber subscriber = topicSessionSubscriber.createSubscriber(topic);
		// since the subscriber only receives
		// messages published *after* the subscription operation, we must
		// create the subscriber now
		Thread.sleep(4000);

		TopicPublisher topicPublisher = topicSession.createPublisher(topic);
		for (int i = 0; i < MAX_MESSAGES; i++) {
			TextMessage message = topicSession.createTextMessage();
			message.setText("message #" + i);
			topicPublisher.publish(message);
		}

		topicConnectionSubscriber.start();

		int i;
		for (i = 0; i < MAX_MESSAGES; i++) {
			Message received = subscriber.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
			Assert.assertFalse(received.getJMSRedelivered());
			if (i == CLIENT_ACK_UNTIL) {
				received.acknowledge();
			}
		}

		Thread.sleep(1000);
		topicSessionSubscriber.recover();

		// for client acknowledge, we should only get messages after
		// CLIENT_ACK_UNTIL
		for (i = CLIENT_ACK_UNTIL + 1; i < MAX_MESSAGES; i++) {
			Message received = subscriber.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
			Assert.assertTrue(received.getJMSRedelivered());
		}

		// check there is no other message
		Message received = subscriber.receive(5000);
		Assert.assertNull(received);

	}

	@Test
	public void testTransaction() throws Exception {

		System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();
		TopicConnectionFactory topicConnectionFactoryA = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic1 = (Topic) jndiContext.lookup("topic.Topic1");
		final Topic topic2 = (Topic) jndiContext.lookup("topic.Topic2");
		TopicConnection topicConnectionA = topicConnectionFactoryA.createTopicConnection();
		final TopicSession topicSessionA = topicConnectionA.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

		TopicConnectionFactory topicConnectionFactoryB = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		TopicConnection topicConnectionB = topicConnectionFactoryB.createTopicConnection();
		final TopicSession topicSessionB = topicConnectionB.createTopicSession(false, Session.SESSION_TRANSACTED);

		final TopicSubscriber subscriberToTopic1 = topicSessionB.createSubscriber(topic1);
		final TopicSubscriber subscriberToTopic2 = topicSessionB.createSubscriber(topic2);
		final TopicPublisher publisherB = topicSessionB.createPublisher(topic1);

		// since the subscriber only receives
		// messages published *after* the subscription operation, we must
		// create the subscribers now
		Thread.sleep(4000);

		TopicPublisher topicPublisher = topicSessionA.createPublisher(topic1);
		for (int i = 0; i < MAX_MESSAGES; i++) {
			TextMessage message = topicSessionA.createTextMessage();
			message.setText("message #" + i);
			topicPublisher.publish(message);
		}

		topicConnectionB.start();

		int i;
		for (i = 0; i < MAX_MESSAGES; i++) {
			Message received = subscriberToTopic1.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
			Assert.assertFalse(received.getJMSRedelivered());
			publisherB.send(topic2, topicSessionA.createTextMessage("resending " + ((TextMessage) received).getText()));
			if (i == COMMIT_INDEX) {
				topicSessionB.commit();
			}
		}

		Thread.sleep(1000);
		topicSessionB.rollback();

		// for client acknowledge, we should only get messages after
		// COMMIT_INDEX
		for (i = COMMIT_INDEX + 1; i < MAX_MESSAGES; i++) {
			Message received = subscriberToTopic1.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
			Assert.assertTrue(received.getJMSRedelivered());
		}

		// check there is no other message for topic 1
		Message received = subscriberToTopic1.receive(5000);
		Assert.assertNull(received);

		// check that pending messages to send were sent upon commit and that
		// others were not sent
		for (i = 0; i <= COMMIT_INDEX; i++) {
			received = subscriberToTopic2.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("resending message #" + i, ((TextMessage) received).getText());
			// Assert.assertFalse(received.getJMSRedelivered());
		}

		// check that messages > COMMIT_INDEX were not sent since there they
		// were not committed
		received = subscriberToTopic2.receive(5000);
	}
}
