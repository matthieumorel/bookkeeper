package org.apache.hedwig.jms.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.util.JMSUtils;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.google.protobuf.ByteString;

public class HedwigJMSObjectMessage extends HedwigJMSMessage implements ObjectMessage {

    Serializable object = null;

    public HedwigJMSObjectMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
        // TODO Auto-generated constructor stub
    }

    public HedwigJMSObjectMessage(HedwigSession hedwigSession, ByteString subscriberId, Message hedwigMessage) {
        super(hedwigSession, subscriberId, hedwigMessage);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void setObject(Serializable object) throws JMSException {
        checkBodyWriteable();
        this.object = object;
    }

    @Override
    public Serializable getObject() throws JMSException {
        if (object == null) {
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(new ByteArrayInputStream(hedwigMessage.getBody().asReadOnlyByteBuffer()
                        .array()));
                object = (Serializable) in.readObject();
            } catch (IOException e) {
                JMSUtils.createJMSException("Cannot read object from hedwig message", e);
            } catch (ClassNotFoundException e) {
                JMSUtils.createJMSException("Cannot deserialize object from hedwig message", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
        return object;
    }

    @Override
    public void doPrepareForSend() throws JMSException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
        } catch (IOException e) {
            JMSUtils.createJMSException("Cannot serialize object " + object, e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignored) {
                }
            }
        }
        builder.setBody(ByteString.copyFrom(bos.toByteArray()));
    }

    @Override
    public JmsBodyType getBodyType() {
        return JmsBodyType.OBJECT;
    }
}
