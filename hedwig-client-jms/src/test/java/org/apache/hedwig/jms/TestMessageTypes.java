package org.apache.hedwig.jms;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
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

public class TestMessageTypes extends HedwigJMSBaseTest {

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
    public void testTextMessage() throws Exception {
        initializeSimplePubSub();
        TextMessage message = topicSession.createTextMessage();
        String text = "message @" + System.currentTimeMillis();
        message.setText(text);
        topicPublisher.publish(message);

        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof TextMessage);
        Assert.assertEquals(text, ((TextMessage) received).getText());
    }

    @Test
    public void testBytesMessage() throws Exception {
        initializeSimplePubSub();
        BytesMessage message = topicSession.createBytesMessage();
        String text = "message @" + System.currentTimeMillis();
        message.writeBytes(text.getBytes());
        message.writeShort((short) 42);
        int textBytesLength = text.getBytes().length;
        topicPublisher.publish(message);

        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof BytesMessage);
        byte[] read = new byte[textBytesLength];
        ((BytesMessage) received).readBytes(read);
        Assert.assertEquals(text, new String(read));
        Assert.assertTrue(((short) 42) == ((BytesMessage) received).readShort());
    }

    @Test
    public void testMapMessage() throws Exception {
        initializeSimplePubSub();
        MapMessage message = topicSession.createMapMessage();
        message.setBoolean("a", Boolean.TRUE);
        long time = System.currentTimeMillis();
        message.setLong("time", time);
        message.setString("timeAsString", String.valueOf(time));

        topicPublisher.publish(message);

        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof MapMessage);
        Assert.assertEquals(Boolean.TRUE, ((MapMessage) received).getBoolean("a"));
        Assert.assertEquals(time, ((MapMessage) received).getLong("time"));
        Assert.assertEquals(String.valueOf(time), ((MapMessage) received).getString("timeAsString"));

    }

    @Test
    public void testObjectMessage() throws Exception {
        initializeSimplePubSub();
        ObjectMessage message = topicSession.createObjectMessage();
        long l1 = System.currentTimeMillis();
        final long l2 = System.currentTimeMillis() + 1;

        Custom object = new Custom();
        object.number = l1;
        object.map = new HashMap<String, Long>() {
            {
                put("l2", l2);
            }
        };

        message.setObject(object);

        topicPublisher.publish(message);

        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof ObjectMessage);
        Assert.assertTrue(((ObjectMessage) message).getObject() instanceof Custom);
        Custom receivedObject = (Custom) ((ObjectMessage) message).getObject();
        Assert.assertEquals(l1, receivedObject.number);
        Assert.assertEquals(new HashMap<String, Long>() {
            {
                put("l2", l2);
            }
        }, receivedObject.map);

    }

    @Test
    public void testStreamMessage() throws Exception {
        initializeSimplePubSub();
        StreamMessage message = topicSession.createStreamMessage();
        String text = "message @" + System.currentTimeMillis();
        message.writeString(text);
        message.writeShort((short) 42);
        String text2 = "message2 @" + System.currentTimeMillis();
        int text2BytesLength = text2.getBytes().length;
        message.writeBytes(text2.getBytes());
        topicPublisher.publish(message);

        topicConnectionSubscriber.start();

        Message received = subscriber.receive(1000);
        Assert.assertTrue(received instanceof StreamMessage);
        Assert.assertEquals(text, ((StreamMessage) received).readString());
        Assert.assertTrue(((short) 42) == ((StreamMessage) received).readShort());
        byte[] bytesRead = new byte[text2BytesLength];
        ((StreamMessage) received).readBytes(bytesRead);
        Assert.assertEquals(text2, new String(bytesRead));

        boolean endOfStream = false;
        try {
            ((StreamMessage) received).readBoolean();
        } catch (MessageEOFException e) {
            endOfStream = true;
        }
        Assert.assertTrue(endOfStream);

    }

    protected void initializeSimplePubSub() throws ConfigurationException, MalformedURLException, Exception,
            NamingException, JMSException, InterruptedException {
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
        subscriber = topicSessionSubscriber.createSubscriber(topic);
        // since the subscriber only receives
        // messages published *after* the subscription operation, we must
        // create the subscriber now
        Thread.sleep(4000);

        topicPublisher = topicSession.createPublisher(topic);
    }

    static class Custom implements Serializable {

        long number;
        Map<String, Long> map;

    }

}
