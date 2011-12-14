package org.apache.hedwig.selector;

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
import org.apache.hedwig.jms.HedwigJMSBaseTest;
import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Assert;
import org.junit.Test;

public class MessageFilteringTest extends HedwigJMSBaseTest {

    private TopicSession topicSession;
    private TopicConnection topicConnection;
    private Topic topic;
    private TopicConnectionFactory topicConnectionFactoryPublisher;
    private TopicConnectionFactory topicConnectionFactorySubscriber;
    private TopicConnection topicConnectionSubscriber;
    private TopicSession topicSessionSubscriber;
    private TopicSubscriber subscriber;
    private TopicPublisher topicPublisher;

    @Test
    public void testFiltering() throws Exception {
        initializeSimplePubSub("city = 'Barcelona'");
        TextMessage message1 = topicSession.createTextMessage();
        message1.setStringProperty("city", "Saint-Brieuc");
        String text1 = "message @" + System.currentTimeMillis();
        message1.setText(text1);
        topicPublisher.publish(message1);

        TextMessage message2 = topicSession.createTextMessage();
        message2.setStringProperty("city", "Barcelona");
        String text2 = "message @" + System.currentTimeMillis();
        message2.setText(text2);
        topicPublisher.publish(message2);
        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof TextMessage);
        Assert.assertEquals(text2, ((TextMessage) received).getText());

        // no other message
        received = subscriber.receive(1000);
        Assert.assertNull(received);

    }

    protected void initializeSimplePubSub(String messageSelector) throws ConfigurationException, MalformedURLException,
            Exception, NamingException, JMSException, InterruptedException {
        System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        hedwigServer = new PubSubServer(serverConf);
        Context jndiContext = new InitialContext();

        topicConnectionFactoryPublisher = (TopicConnectionFactory) jndiContext.lookup("TopicConnectionFactory");
        topic = (Topic) jndiContext.lookup("topic.Topic1");
        topicConnection = topicConnectionFactoryPublisher.createTopicConnection();
        topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        topicConnectionFactorySubscriber = (TopicConnectionFactory) jndiContext.lookup("TopicConnectionFactory");
        topicConnectionSubscriber = topicConnectionFactorySubscriber.createTopicConnection();
        topicSessionSubscriber = topicConnectionSubscriber.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);
        subscriber = topicSessionSubscriber.createSubscriber(topic, messageSelector, false);

        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        Thread.sleep(4000);

        topicPublisher = topicSession.createPublisher(topic);

    }
}
