package org.apache.hedwig.jms.administered;

import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

public class HedwigTemporaryTopic extends HedwigDestination implements TemporaryTopic {

    String topicName;
    HedwigConnection connection;

    public HedwigTemporaryTopic(HedwigConnection connection) throws JMSException {
        // temporary topic name should be unique for connection
        this(connection, "tmp-" + connection.getClientID() + "-" + UUID.randomUUID().toString());
    }

    public HedwigTemporaryTopic(HedwigConnection connection, String topicName) throws JMSException {
        super();
        // temporary topic name should be unique for connection
        this.topicName = topicName;
    }

    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    @Override
    public void delete() throws JMSException {
        // Currently in Hedwig, resources associated to a temporary topic are
        // automatically released when all subscribers unsubscribe. There is no
        // actual "deletion" of topics.
        // Therefore, this method only checks that there is no subscriber to the
        // topic
        for (HedwigSession session : connection.sessions) {
            if (session.consumers.containsKey(topicName)) {
                throw new JMSException("Cannot delete temporary topic [" + topicName
                        + "] because it has active subscriptions");
            }
        }
    }

    @Override
    public byte getDestinationType() {
        return HedwigDestination.TEMPORARY_TOPIC_DEST_TYPE;
    }

    @Override
    public String[] getDestinationPaths() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isQueue() {
        return false;
    }

    @Override
    public String getPhysicalName() {
        return topicName;
    }

    @Override
    public boolean isTemporary() {
        return true;
    }
}
