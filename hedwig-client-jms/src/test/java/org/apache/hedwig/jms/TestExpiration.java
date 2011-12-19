package org.apache.hedwig.jms;

import javax.jms.DeliveryMode;
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

public class TestExpiration extends HedwigJMSBaseTest {

    @Test
    public void testExpiration() throws Exception {
        System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        hedwigServer = new PubSubServer(serverConf);
        Context jndiContext = new InitialContext();
        TopicConnectionFactory topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");
        Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
        TopicConnection publisherTopicConnection = topicConnectionFactoryPublisher.createTopicConnection();
        TopicSession publisherTopicSession = publisherTopicConnection.createTopicSession(false,
                Session.CLIENT_ACKNOWLEDGE);

        TopicConnectionFactory topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");
        TopicConnection subscriberTopicConnection = topicConnectionFactorySubscriber.createTopicConnection();
        TopicSession subscriberTopicSession = subscriberTopicConnection.createTopicSession(false,
                Session.CLIENT_ACKNOWLEDGE);
        final TopicSubscriber subscriber = subscriberTopicSession.createSubscriber(topic);
        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        publisherTopicConnection.start();
        subscriberTopicConnection.start();
        Thread.sleep(4000);

        TopicPublisher topicPublisher = publisherTopicSession.createPublisher(topic);

        String text2 = "message@" + System.currentTimeMillis();
        String text3 = "message@" + System.currentTimeMillis() + 1;
        TextMessage message = publisherTopicSession.createTextMessage(text2);
        topicPublisher.publish(message, DeliveryMode.PERSISTENT, 0, 1000);

        // message will be expired after reception by JMS client, upon fetching
        // by the client, and will be ignored. We'll fetch the next one instead
        Thread.sleep(2000);
        message = publisherTopicSession.createTextMessage(text3);
        // message.setJMSExpiration(System.currentTimeMillis() + 2000);
        topicPublisher.publish(message);

        Message received = subscriber.receive(1000);

        Assert.assertEquals(text3, ((TextMessage) received).getText());

    }
}
