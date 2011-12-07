package org.apache.hedwig.jms.message;

import javax.jms.JMSException;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;

import com.google.protobuf.ByteString;

public class JMSMessageFactory {

    public static HedwigJMSMessage getMessage(HedwigSession hedwigSession, ByteString subscriberId,
            org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage) throws JMSException {
        JmsBodyType jmsBodyType = hedwigMessage.getJmsBodyType();
        switch (jmsBodyType) {
        case BYTES:
            return new HedwigJMSBytesMessage(hedwigSession, subscriberId, hedwigMessage);
        case OBJECT:
            return new HedwigJMSObjectMessage(hedwigSession, subscriberId, hedwigMessage);
        case MAP:
            return new HedwigJMSMapMessage(hedwigSession, subscriberId, hedwigMessage);
        case STREAM:
            return new HedwigJMSStreamMessage(hedwigSession, subscriberId, hedwigMessage);
        case TEXT:
            return new HedwigJMSTextMessage(hedwigSession, subscriberId, hedwigMessage);
        default:
            throw new JMSException("Cannot identify message type");
        }

    }
}
