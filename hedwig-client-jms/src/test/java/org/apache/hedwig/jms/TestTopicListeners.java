package org.apache.hedwig.jms;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import junit.framework.Assert;

import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Test;

public class TestTopicListeners extends HedwigJMSBaseTest {

    private static final int NB_MESSAGES = 10;

    @Test
    public void testTopicListener() throws Exception {
        System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        hedwigServer = new PubSubServer(serverConf);
        Context jndiContext = new InitialContext();

        TopicConnectionFactory publisherTopicConnectionFactory = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");
        final Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
        TopicConnection publisherTopicConnection = publisherTopicConnectionFactory.createTopicConnection();
        TopicSession publisherTopicSession = publisherTopicConnection.createTopicSession(false,
                Session.CLIENT_ACKNOWLEDGE);

        final CountDownLatch signalReadyToReceive = new CountDownLatch(1);
        final CountDownLatch signalReceived = new CountDownLatch(NB_MESSAGES);
        final CountDownLatch signalReceived2 = new CountDownLatch(NB_MESSAGES + 1);

        TopicConnectionFactory subscriberTopicConnectionFactory = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");

        final TopicConnection subscriberTopicConnection = subscriberTopicConnectionFactory.createTopicConnection();
        final TopicSession subscriberTopicSession = subscriberTopicConnection.createTopicSession(false,
                Session.CLIENT_ACKNOWLEDGE);

        Thread.sleep(4000);

        final TopicSubscriber subscriber = subscriberTopicSession.createSubscriber(topic);
        subscriber.setMessageListener(new TopicListenerA(signalReceived, signalReceived2));
        subscriberTopicConnection.start();
        signalReadyToReceive.countDown();
        signalReadyToReceive.await();

        TopicPublisher topicPublisher = publisherTopicSession.createPublisher(topic);

        for (int i = 0; i < NB_MESSAGES; i++) {
            TextMessage message = publisherTopicSession.createTextMessage();
            message.setText("message");
            topicPublisher.publish(message);
        }
        Assert.assertTrue(signalReceived.await(5, TimeUnit.SECONDS));

        // Test closing the connection
        subscriberTopicConnection.close();

        // publish another message. This one should never be received because
        // the connection is closed
        topicPublisher.publish(publisherTopicSession.createTextMessage("blah"));
        Assert.assertFalse(signalReceived2.await(5, TimeUnit.SECONDS));
    }

    private class TopicListenerA implements MessageListener {

        CountDownLatch signalMessagesReceived;
        CountDownLatch signalMessagesReceived2;

        public TopicListenerA(CountDownLatch signalMessagesReceived, CountDownLatch signalMessagesReceived2) {
            this.signalMessagesReceived = signalMessagesReceived;
            this.signalMessagesReceived2 = signalMessagesReceived2;
        }

        @Override
        public void onMessage(Message arg0) {
            System.out.print(".");
            signalMessagesReceived.countDown();
            signalMessagesReceived2.countDown();
        }

    }
}
