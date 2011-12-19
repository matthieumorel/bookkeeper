package org.apache.hedwig.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;

import junit.framework.Assert;

import org.apache.hedwig.jms.administered.HedwigTopicConnection;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.Test;

public class TestServiceRequests extends HedwigJMSBaseTest {

    @Test
    public void testTopicRequestor() throws Exception {
        System.setProperty(HedwigTopicConnection.HEDWIG_CLIENT_CONFIG_FILE, hedwigConfigFile.getAbsolutePath());

        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(hedwigConfigFile.toURI().toURL());

        hedwigServer = new PubSubServer(serverConf);
        Context jndiContext = new InitialContext();

        TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) jndiContext
                .lookup("TopicConnectionFactory");
        TopicConnection connection1 = topicConnectionFactory.createTopicConnection();
        TopicSession session1 = connection1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = (Topic) jndiContext.lookup("topic.Topic1");
        TopicSubscriber subscriber1 = session1.createSubscriber(topic);
        subscriber1.setMessageListener(new RequestListener(session1, topic));

        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        Thread.sleep(4000);

        connection1.start();

        TopicRequestor topicRequestor = new TopicRequestor(session1, topic);
        String text = "text@" + System.currentTimeMillis();
        TextMessage msg = session1.createTextMessage(text);

        Message reply = topicRequestor.request(msg);

        Assert.assertEquals(text + "-reply", ((TextMessage) reply).getText());

    }

    private class RequestListener implements MessageListener {

        private TopicSession s;
        private TopicPublisher replier;

        public RequestListener(TopicSession s, Topic t) throws JMSException {
            this.s = s;
            replier = s.createPublisher(t);
        }

        @Override
        public void onMessage(Message message) {
            try {
                Topic replyTopic = (Topic) message.getJMSReplyTo();
                replier.publish(replyTopic, s.createTextMessage(((TextMessage) message).getText() + "-reply"));
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
