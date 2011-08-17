package org.apache.hedwig.jms;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import javax.naming.ConfigurationException;
import javax.naming.Context;
import javax.naming.InitialContext;

import junit.framework.AssertionFailedError;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.client.netty.HedwigPublisher;
import org.apache.hedwig.client.netty.HedwigSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestSimplePubSub extends HedwigJMSBaseTest {

	static final int MAX_MESSAGES = 1000;

	@Test
	public void testTopicProducerConsumer() throws Exception {
		


		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.loadConf(hedwigConfigFile.toURI()
		        .toURL());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI()
		        .toURL());

		hedwigServer = new PubSubServer(serverConf);
		Context jndiContext = new InitialContext();
		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSession = topicConnection.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);

		TopicConnectionFactory topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		TopicConnection topicConnectionSubscriber = topicConnectionFactorySubscriber.createTopicConnection();
		TopicSession topicSessionSubscriber = topicConnectionSubscriber.createTopicSession(true,
		        Session.CLIENT_ACKNOWLEDGE);
		TopicSubscriber subscriber = null;
		// since the subscriber only receives
		// messages published *after* the subscription operation, we must
		// create the subscriber now
		subscriber = topicSessionSubscriber.createSubscriber(topic);
		Thread.sleep(4000);

		TopicPublisher topicPublisher = topicSession.createPublisher(topic);
		for (int i = 0; i < MAX_MESSAGES; i++) {
			TextMessage message = topicSession.createTextMessage();
			message.setText("message #" + i);
			topicPublisher.publish(message);
		}

		// make sure no message is received until we "start" the connection
		// Message received = subscriber.receive(1000);
		// Assert.assertTrue(received ==null);

		topicConnectionSubscriber.start();
		int i = 0;
		for (i = 0; i < MAX_MESSAGES; i++) {
			Message received = subscriber.receive(1000);
			Assert.assertTrue(received instanceof TextMessage);
			Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
		}
		Assert.assertEquals(MAX_MESSAGES, i);

	}

	@Test
	public void testNoDeliveryUntilConnectionStarted() throws Exception, MalformedURLException {
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.loadConf(hedwigConfigFile.toURI()
		        .toURL());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI()
		        .toURL());

		hedwigServer = new PubSubServer(serverConf);
		final Context jndiContext = new InitialContext();
		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		final Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSession = topicConnection.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);

		final CountDownLatch signalReadyToReceive = new CountDownLatch(1);
		final CountDownLatch signalReceived = new CountDownLatch(1);
		TopicConnectionFactory subscriberTopicConnectionFactory = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		final TopicConnection subscriberTopicConnection = subscriberTopicConnectionFactory.createTopicConnection();
		TopicSession subscriberTopicSession = subscriberTopicConnection.createTopicSession(true,
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
	

}
