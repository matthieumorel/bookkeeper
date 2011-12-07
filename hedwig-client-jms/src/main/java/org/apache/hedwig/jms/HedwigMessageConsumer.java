package org.apache.hedwig.jms;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.util.ClientIdGenerator;
import org.apache.hedwig.jms.util.JMSUtils;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class HedwigMessageConsumer implements MessageConsumer, MessageHandler {

    protected ByteString topicName;
    protected ByteString subscriberId;
    protected HedwigSession hedwigSession;
    Lock connectionStateLock = new ReentrantLock();
    MessageListener messageListener;
    private HedwigClient hedwigClient;

    public HedwigMessageConsumer(HedwigSession session, ByteString topicName, ClientConfiguration hedwigClientConfig) {
        this.topicName = topicName;
        this.hedwigSession = session;
        this.subscriberId = ByteString.copyFromUtf8(ClientIdGenerator.getNewClientId());
        try {
            this.hedwigClient = new HedwigClient(hedwigClientConfig);

            hedwigClient.getSubscriber().subscribe(topicName, subscriberId, CreateOrAttach.CREATE_OR_ATTACH);
            hedwigSession.addConsumer(subscriberId, this);
        } catch (CouldNotConnectException e) {
            e.printStackTrace();
        } catch (ClientAlreadySubscribedException e) {
            e.printStackTrace();
        } catch (ServiceDownException e) {
            e.printStackTrace();
        } catch (InvalidSubscriberIdException e) {
            e.printStackTrace();
        }
    }

    private void checkSessionNotClosed() throws IllegalStateException {
        if (hedwigSession.isClosed()) {
            throw new IllegalStateException("Session is closed");
        }
    }

    public HedwigClient getHedwigClient() {
        return hedwigClient;
    }

    public ByteString getSubscriberId() {
        return subscriberId;
    }

    public ByteString getHedwigTopicName() {
        return topicName;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkSessionNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkSessionNotClosed();
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkSessionNotClosed();
        this.messageListener = listener;
        hedwigSession.addListener(subscriberId, listener);
    }

    @Override
    public Message receive() throws JMSException {
        return doReceive(0);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        return doReceive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return doReceive(-1);
    }

    private Message doReceive(long timeout) throws JMSException {
        checkSessionNotClosed();
        try {
            hedwigSession.getHedwigConnection().waitUntilStarted();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        HedwigJMSMessage retrieved = null;
        if (timeout == 0) {
            retrieved = hedwigSession.takeNextMessage(subscriberId);
        } else if (timeout > 0) {
            retrieved = hedwigSession.pollNextMessage(subscriberId, timeout, TimeUnit.MILLISECONDS);
        } else {
            retrieved = hedwigSession.pollNextMessage(subscriberId, 0, TimeUnit.MILLISECONDS);
        }
        if (retrieved != null) {
            hedwigSession.messageSuccessfullyDelivered(retrieved);
            return retrieved;
        } else {
            return null;
        }

    }

    @Override
    public void close() throws JMSException {
        try {
            getHedwigClient().getSubscriber().unsubscribe(topicName, subscriberId);
        } catch (ClientNotSubscribedException e) {
            throw JMSUtils.createJMSException(
                    "Internal error: this consumer is not subscribed to the broker with topic "
                            + topicName.toStringUtf8() + "]", e);
        } catch (CouldNotConnectException e) {
            throw JMSUtils.createJMSException(
                    "Internal error: cannot connect to the broker for propertly closing this consumer", e);
        } catch (ServiceDownException e) {
            throw JMSUtils.createJMSException("Internal error: broker is down", e);
        } catch (InvalidSubscriberIdException e) {
            throw JMSUtils.createJMSException("Internal error: wrong client id", e);
        }

    }

    @Override
    public synchronized void consume(ByteString topic, ByteString subscriberId,
            org.apache.hedwig.protocol.PubSubProtocol.Message msg, Callback<Void> callback, Object context) {
        if (this.topicName.equals(topic) && this.subscriberId.equals(subscriberId)) {
            // serialize access to messages through the session
            try {
                hedwigSession.offerReceivedMessage(msg, topicName, subscriberId);
            } catch (JMSException e) {
                LoggerFactory.getLogger(getClass().getName()).error("Cannot read message", e);
            }
        }

    }

    public void acknowledge(MessageSeqId messageId) throws JMSException {
        checkSessionNotClosed();
        try {
            // tell hedwig
            hedwigClient.getSubscriber().consume(topicName, subscriberId, messageId);
            hedwigSession.acknowledged(messageId);
        } catch (ClientNotSubscribedException e) {
            throw JMSUtils
                    .createJMSException(
                            "Cannot acknowledge message because this client is not subscribed to the corresponding destination",
                            e);
        }
    }

    public void start() throws JMSException {
        try {
            getHedwigClient().getSubscriber().startDelivery(topicName, subscriberId, this);
        } catch (ClientNotSubscribedException e) {
            throw JMSUtils.createJMSException("Cannot start delivery of messages", e);
        }
    }

    public void stop() throws JMSException {
        try {
            getHedwigClient().getSubscriber().stopDelivery(topicName, subscriberId);
        } catch (ClientNotSubscribedException e) {
            throw JMSUtils.createJMSException("Cannot stop delivery of messages", e);
        }
    }

}
