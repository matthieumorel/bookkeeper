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
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Assert;
import org.junit.Test;

public class TestACK extends HedwigJMSBaseTest {

	private static final int MAX_MESSAGES = 1000;
	private static final int CLIENT_ACK_UNTIL = 42;

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
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.loadConf(hedwigConfigFile.toURI().toURL());

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

		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.loadConf(hedwigConfigFile.toURI().toURL());

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
}
