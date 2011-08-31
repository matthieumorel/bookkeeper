package org.apache.hedwig.jms;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;
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

import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Assert;
import org.junit.Test;

public class TestSimplePubSub extends HedwigJMSBaseTest {

	static final int MAX_MESSAGES = 1000;
	private TopicSession subscriberTopicSession;
	private TopicSession publisherTopicSession;
	private TopicConnection subscriberTopicConnection;
	private TopicConnection publisherTopicConnection;
	private TopicPublisher topicPublisher;
	private TopicSubscriber subscriber;

	@Test
	public void testTopicProducerConsumer() throws Exception {

		System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();
		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		publisherTopicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		publisherTopicSession = publisherTopicConnection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);

		TopicConnectionFactory topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		subscriberTopicConnection = topicConnectionFactorySubscriber.createTopicConnection();
		subscriberTopicSession = subscriberTopicConnection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);
		subscriber = subscriberTopicSession.createSubscriber(topic);
		// since the subscriber only receives
		// messages published *after* the subscription operation, we must
		// create the subscriber now
		Thread.sleep(4000);

		topicPublisher = publisherTopicSession.createPublisher(topic);
		for (int i = 0; i < MAX_MESSAGES; i++) {
			TextMessage message = publisherTopicSession.createTextMessage();
			message.setText("message #" + i);
			System.out.println("Sent message #" + i);
			topicPublisher.publish(message);
		}

		final CountDownLatch signalMessageReceived = new CountDownLatch(1);
		// make sure no message is received until we "start" the connection
		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				try {
					Message received = subscriber.receive(1000);
					if (received != null) {
						signalMessageReceived.countDown();
					}
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, 0);

		Assert.assertFalse(signalMessageReceived.await(2, TimeUnit.SECONDS));
		subscriberTopicConnection.start();
		Assert.assertTrue(signalMessageReceived.await(2, TimeUnit.SECONDS));
		int i;
		for (i = 1; i < MAX_MESSAGES; i++) {
			Message received = subscriber.receive(2000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
		}
		Assert.assertEquals(MAX_MESSAGES, i);

	}

	@Test
	public void testNoDeliveryUntilConnectionStarted() throws Exception, MalformedURLException {
		System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();

		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSession = topicConnection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);

		final CountDownLatch signalReadyToReceive = new CountDownLatch(1);
		final CountDownLatch signalReceived = new CountDownLatch(1);
		TopicConnectionFactory subscriberTopicConnectionFactory = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		TopicConnection subscriberTopicConnection = subscriberTopicConnectionFactory.createTopicConnection();
		TopicSession subscriberTopicSession = subscriberTopicConnection.createTopicSession(false,
		        Session.CLIENT_ACKNOWLEDGE);
		final TopicSubscriber subscriber = subscriberTopicSession.createSubscriber(topic);
		Thread.sleep(4000);
		final List<Message> receivedMessagePlaceholder = new ArrayList<Message>();

		Thread subscriberThread = new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					signalReadyToReceive.countDown();
					receivedMessagePlaceholder.add(subscriber.receive(1000));
				} catch (JMSException ignored) {
				}
				signalReceived.countDown();

			}
		});
		subscriberThread.start();

		signalReadyToReceive.await();

		TopicPublisher topicPublisher = topicSession.createPublisher(topic);
		TextMessage message = topicSession.createTextMessage();
		message.setText("message");
		topicPublisher.publish(message);

		// no message should be received because the connection is not started
		Assert.assertFalse(signalReceived.await(5, TimeUnit.SECONDS));

		// now we start the connection and expect to receive something
		subscriberTopicConnection.start();

		Assert.assertTrue(signalReceived.await(5, TimeUnit.SECONDS));
		Message receivedMessage = receivedMessagePlaceholder.iterator().next();
		Assert.assertTrue(receivedMessage instanceof TextMessage);
		Assert.assertEquals("message", ((TextMessage) receivedMessage).getText());

	}

	@Test
	public void testConnectionStop() throws Exception {
		testTopicProducerConsumer();
		TextMessage message = publisherTopicSession.createTextMessage("blah");
		subscriberTopicConnection.close();
		boolean cannotReceive = false;
		try {
			subscriber.receive();
		} catch (IllegalStateException e) {
			cannotReceive = true;
		}
		Assert.assertTrue(cannotReceive);

		publisherTopicConnection.close();
		boolean cannotSend = false;
		try {
			topicPublisher.send(message);
		} catch (IllegalStateException e) {
			cannotSend = true;
		}
		Assert.assertTrue(cannotSend);
	}

}
