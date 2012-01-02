package org.apache.hedwig.jms;

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

import junit.framework.Assert;

import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Test;

public class TestDurableTopicSubscriber extends HedwigJMSBaseTest {

    private static final int MAX_MESSAGES = 100;
    private static final int NB_MESSAGES_1 = 42;

    @Test
    public void testDurableSubscriber() throws Exception {

        System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        hedwigServer = new PubSubServer(serverConf);
        Context jndiContext = new InitialContext();

        TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");
        Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
        TopicConnection topicConnection1 = topicConnectionFactory.createTopicConnection();
        TopicSession topicSession1 = topicConnection1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        TopicConnection topicConnection2 = topicConnectionFactory.createTopicConnection();
        TopicSession topicSession2 = topicConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durableSubscriber1 = topicSession2.createDurableSubscriber(topic, "durableSubscriber1");
        // TopicSubscriber durableSubscriber1 =
        // topicSession2.createSubscriber(topic);
        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        Thread.sleep(4000);

        topicConnection1.start();
        topicConnection2.start();

        TopicPublisher topicPublisher = topicSession1.createPublisher(topic);

        for (int i = 0; i < MAX_MESSAGES; i++) {
            TextMessage message = topicSession1.createTextMessage();
            message.setText("message #" + i);
            topicPublisher.publish(message);
        }
        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        Thread.sleep(4000);
        int i;
        for (i = 0; i < NB_MESSAGES_1; i++) {
            Message received = durableSubscriber1.receive(1000);
            Assert.assertTrue(received instanceof TextMessage);
            Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
        }

        durableSubscriber1.close();
        topicConnection2.close();

        TopicConnection topicConnection3 = topicConnectionFactory.createTopicConnection();
        TopicSession topicSession3 = topicConnection3.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durableSubscriber2 = topicSession3.createDurableSubscriber(topic, "durableSubscriber1");
        topicConnection3.start();
        for (i = NB_MESSAGES_1; i < MAX_MESSAGES - 1; i++) {
            Message received = durableSubscriber2.receive(1000);
            Assert.assertTrue(received instanceof TextMessage);
            Assert.assertEquals("message #" + i, ((TextMessage) received).getText());
        }

        // unsubscribing will clear pending messages for this durable
        // subscription
        durableSubscriber2.close();
        topicSession3.unsubscribe("durableSubscriber1");
        topicSession3.close();

        TopicConnection topicConnection4 = topicConnectionFactory.createTopicConnection();
        TopicSession topicSession4 = topicConnection4.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber durableSubscriber3 = topicSession4.createDurableSubscriber(topic, "durableSubscriber1");
        topicConnection4.start();
        for (i = MAX_MESSAGES - 1; i < MAX_MESSAGES; i++) {
            Message received = durableSubscriber3.receive(5000);
            Assert.assertNull(received);
        }

    }

}
