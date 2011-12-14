package org.apache.hedwig.jms.administered;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.commons.lang.NotImplementedException;

public class HedwigTopicConnection extends HedwigConnection implements TopicConnection {

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new NotImplementedException(
                "Hedwig currently does not support the JMS optional operation 'createConnectionConsumer'");
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgementMode) throws JMSException {
        return new HedwigTopicSession(this, acknowledgementMode, hedwigClientConfig);
    }

}
