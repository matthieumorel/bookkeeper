package org.apache.hedwig.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
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

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Test;

public class TestTopicListeners extends HedwigJMSBaseTest {

	private static final int NB_MESSAGES = 10;

	@Test
	public void testTopicListener() throws Exception {
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.loadConf(hedwigConfigFile.toURI().toURL());

		ServerConfiguration serverConf = new ServerConfiguration();
		serverConf.loadConf(hedwigConfigFile.toURI().toURL());

		hedwigServer = new PubSubServer(serverConf);
		final Context jndiContext = new InitialContext();
		TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
		        .lookup("TopicConnectionFactory");
		final Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
		TopicConnection topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
		TopicSession topicSession = topicConnection.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);

		final CountDownLatch signalReadyToReceive = new CountDownLatch(1);
		final CountDownLatch signalReceived = new CountDownLatch(NB_MESSAGES);

		Thread.sleep(4000);

		final List<Message> receivedMessagePlaceholder = new ArrayList<Message>();

		Thread subscriberThread = new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					TopicConnectionFactory subscriberTopicConnectionFactory = (TopicConnectionFactory) jndiContext
					        .lookup("TopicConnectionFactory");
					final TopicConnection subscriberTopicConnection = subscriberTopicConnectionFactory
					        .createTopicConnection();
					TopicSession subscriberTopicSession = subscriberTopicConnection.createTopicSession(true,
					        Session.CLIENT_ACKNOWLEDGE);
					final TopicSubscriber subscriber = subscriberTopicSession.createSubscriber(topic);
					subscriber.setMessageListener(new TopicListenerA(signalReceived));
					signalReadyToReceive.countDown();
				} catch (JMSException ignored) {
				} catch (NamingException e) {
				}

			}
		});
		subscriberThread.start();

		signalReadyToReceive.await();

		TopicPublisher topicPublisher = topicSession.createPublisher(topic);

		for (int i = 0; i < NB_MESSAGES; i++) {
			TextMessage message = topicSession.createTextMessage();
			message.setText("message");
			topicPublisher.publish(message);
		}
		signalReceived.await(5, TimeUnit.SECONDS);
	}

	// @Test
	// public void testMultipleTopicListeners() throws Exception {
	// ClientConfiguration clientConf = new ClientConfiguration();
	// clientConf.loadConf(hedwigConfigFile.toURI().toURL());
	//
	// ServerConfiguration serverConf = new ServerConfiguration();
	// serverConf.loadConf(hedwigConfigFile.toURI().toURL());
	//
	// hedwigServer = new PubSubServer(serverConf);
	// final Context jndiContext = new InitialContext();
	// TopicConnectionFactory topicConnectionFactoryPublisher =
	// (TopicConnectionFactory) jndiContext
	// .lookup("TopicConnectionFactory");
	// final Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
	// TopicConnection topicConnection =
	// topicConnectionFactoryPublisher.createTopicConnection();
	// TopicSession topicSession = topicConnection.createTopicSession(true,
	// Session.CLIENT_ACKNOWLEDGE);
	//
	// final CountDownLatch signalReadyToReceive = new CountDownLatch(1);
	// final CountDownLatch signalReceived = new CountDownLatch(NB_MESSAGES);
	//
	// Thread.sleep(4000);
	//
	// final List<Message> receivedMessagePlaceholder = new
	// ArrayList<Message>();
	//
	// Thread subscriberThread = new Thread(new Runnable() {
	//
	// @Override
	// public void run() {
	//
	// try {
	// TopicConnectionFactory subscriberTopicConnectionFactory =
	// (TopicConnectionFactory) jndiContext
	// .lookup("TopicConnectionFactory");
	// final TopicConnection subscriberTopicConnection =
	// subscriberTopicConnectionFactory
	// .createTopicConnection();
	// TopicSession subscriberTopicSession =
	// subscriberTopicConnection.createTopicSession(true,
	// Session.CLIENT_ACKNOWLEDGE);
	// final TopicSubscriber subscriber =
	// subscriberTopicSession.createSubscriber(topic);
	// subscriber.setMessageListener(new TopicListenerA(signalReceived));
	// signalReadyToReceive.countDown();
	// } catch (JMSException ignored) {
	// } catch (NamingException e) {
	// }
	//
	// }
	// });
	// subscriberThread.start();
	//
	// signalReadyToReceive.await();
	//
	// TopicPublisher topicPublisher = topicSession.createPublisher(topic);
	//
	// for (int i = 0; i < NB_MESSAGES; i++) {
	// TextMessage message = topicSession.createTextMessage();
	// message.setText("message");
	// topicPublisher.publish(message);
	// }
	// signalReceived.await(5, TimeUnit.SECONDS);
	// }

	private class TopicListenerA implements MessageListener {

		CountDownLatch signalMessagesReceived;

		public TopicListenerA(CountDownLatch signalMessagesReceived) {
			this.signalMessagesReceived = signalMessagesReceived;
		}

		@Override
		public void onMessage(Message arg0) {
			signalMessagesReceived.countDown();
		}

	}
}
