package org.apache.hedwig.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.util.Callback;
import org.apache.log4j.Logger;

public class HedwigTopicPublisher extends HedwigMessageProducer implements TopicPublisher {

    public HedwigTopicPublisher(HedwigSession hedwigSession, Topic topic) {
        super(hedwigSession, topic);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) getDestination();
    }

    @Override
    public void publish(Message message) throws JMSException {
        send(message);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        send(topic, message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send(topic, message, deliveryMode, priority, timeToLive);
    }

    public class PublisherCallback implements Callback<Void> {

        String topicName;

        public PublisherCallback(String topicName) {
            this.topicName = topicName;
        }

        @Override
        public void operationFinished(Object ctx, Void resultOfOperation) {
            Logger.getLogger(PublisherCallback.class).info("Successfully sent message on topic " + topicName);
        }

        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
            Logger.getLogger(PublisherCallback.class)
                    .error("Failed to send message on topic : " + topicName, exception);
        }

    }

}
