package org.apache.hedwig.jms.message;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.google.protobuf.ByteString;

public class HedwigJMSTextMessage extends HedwigJMSMessage implements TextMessage {

    public HedwigJMSTextMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
    }

    public HedwigJMSTextMessage(HedwigSession hedwigSession, ByteString subscriberId, Message hedwigMessage) {
        super(hedwigSession, subscriberId, hedwigMessage);
    }

    @Override
    public String getText() throws JMSException {
        return hedwigMessage.getBody().toStringUtf8();
    }

    @Override
    public void setText(String text) throws JMSException {
        checkBodyWriteable();
        builder.setBody(ByteString.copyFromUtf8(text));
    }

    @Override
    public void doPrepareForSend() {
        // nothing to do
    }

    @Override
    public JmsBodyType getBodyType() {
        return JmsBodyType.TEXT;
    }

}
