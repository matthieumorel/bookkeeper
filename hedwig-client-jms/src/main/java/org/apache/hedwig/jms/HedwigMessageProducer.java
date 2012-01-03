package org.apache.hedwig.jms;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HedwigMessageProducer implements MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(HedwigMessageProducer.class);

    protected HedwigSession hedwigSession;
    private Destination defaultDestination;
    private int defaultPriority = 0;
    private int defaultDeliveryMode = DeliveryMode.PERSISTENT;
    private long defaultTimeToLive = 10000;

    public HedwigMessageProducer(HedwigSession hedwigSession, Destination destination) {
        this.hedwigSession = hedwigSession;
        this.defaultDestination = destination;
    }

    @Override
    public void close() throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getDeliveryMode() throws JMSException {
        return defaultDeliveryMode;
    }

    @Override
    public Destination getDestination() throws JMSException {
        return defaultDestination;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getPriority() throws JMSException {
        return defaultPriority;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        return defaultTimeToLive;
    }

    @Override
    public void send(Message message) throws JMSException {
        send(getDestination(), message);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(getDestination(), message, deliveryMode, priority, timeToLive);

    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        // Note: delivery mode is ignored, since all messages are persistent in
        // Hedwig
        // FIXME message may be lost however in the async comm

        // TODO priorities: there is no such notion in Hedwig. However, the JMS
        // spec asks ("[provider] should do its best")
        // ... to at least deliver high priority messages ahead of low
        // priority ones.
        // TODO ttl: JMS spec says that
        // "it is not acceptable to ignore time-to-live"
        message.setJMSTimestamp(System.currentTimeMillis());
        // JMS: If the time-to-live is specified as zero, expiration is set to
        // zero to indicate that the message does not expire.
        message.setJMSExpiration(timeToLive == 0 ? 0 : message.getJMSTimestamp() + timeToLive);

        hedwigSession.send(destination, (HedwigJMSMessage) message);

    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        if (!(DeliveryMode.PERSISTENT == deliveryMode || DeliveryMode.NON_PERSISTENT == deliveryMode)) {
            throw new JMSException("Invalid delivery mode", IllegalArgumentException.class.getName());
        }
        if (DeliveryMode.NON_PERSISTENT == deliveryMode) {
            logger.debug("Specifying a non-persistent delivery mode has no effect in Hedwig's JMS implementation "
                    + "because all messages are persistent. Messages will be delivered once-and-only-once "
                    + "(if they are not destructed).");
        }
        this.defaultDeliveryMode = deliveryMode;
    }

    @Override
    public void setDisableMessageID(boolean arg0) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDisableMessageTimestamp(boolean arg0) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPriority(int priority) throws JMSException {
        this.defaultPriority = priority;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.defaultTimeToLive = timeToLive;
    }

}
