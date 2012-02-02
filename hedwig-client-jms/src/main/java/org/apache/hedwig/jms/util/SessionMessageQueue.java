package org.apache.hedwig.jms.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.administered.HedwigTopic;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.message.JMSMessageFactory;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;

/**
 * Serializes messages in a session
 *
 */
public class SessionMessageQueue {

    ListMultimap<ByteString, HedwigJMSMessage> orderedReceivedMessagesBySubscriber = ArrayListMultimap.create();
    ReentrantLock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();
    Map<ByteString, Condition> subscribersNotEmptyConditions = new HashMap<ByteString, Condition>();
    Map<MessageSeqId, TopicNameAndSubscriberId> undeliveredMessageIds = new HashMap<PubSubProtocol.MessageSeqId, TopicNameAndSubscriberId>();
    Map<MessageSeqId, TopicNameAndSubscriberId> unacknowledgedDeliveredMessageIds = new HashMap<PubSubProtocol.MessageSeqId, TopicNameAndSubscriberId>();
    MessageSeqId lastUnacknowledged = null;
    boolean recovering = false;
    HedwigSession hedwigSession;
    List<MessageWithDestination> pendingMessagesToSend = new ArrayList<SessionMessageQueue.MessageWithDestination>();

    public SessionMessageQueue(HedwigSession hedwigSession) {
        this.hedwigSession = hedwigSession;
    }

    public void addConsumer(ByteString subscriberId) {
        subscribersNotEmptyConditions.put(subscriberId, lock.newCondition());
    }

    public void recoveryInitiated() {
        recovering = true;
    }

    // NOTE: currently, upon failure in the commit operation, there is no
    // rollback
    public void commit() throws JMSException {
        // 1. acknowledge all received messages
        undeliveredMessageIds.clear();

        if (lastUnacknowledged != null) {
            hedwigSession.getConsumer(unacknowledgedDeliveredMessageIds.get(lastUnacknowledged).getSubscriberId())
                    .acknowledge(lastUnacknowledged);
            lastUnacknowledged = null;
        }

        // 2. send all pending messages
        Iterator<MessageWithDestination> iterator = pendingMessagesToSend.iterator();
        while (iterator.hasNext()) {

            MessageWithDestination next = iterator.next();

            // TODO async publish?
            try {
                hedwigSession
                        .getHedwigProducerForSession()
                        .getPublisher()
                        .publish(ByteString.copyFromUtf8(((HedwigTopic) next.getDestination()).getTopicName()),
                                next.getMessage().getHedwigMessage());
            } catch (CouldNotConnectException e) {
                throw JMSUtils.createJMSException("Cannot send pending message while committing transaction", e);
            } catch (ServiceDownException e) {
                throw JMSUtils.createJMSException("Cannot send pending message while committing transaction", e);
            }
        }
        pendingMessagesToSend.clear();

    }

    public void clearPendingMessages() throws JMSException {
        // destroy pending messages to send
        pendingMessagesToSend.clear();
    }

    public void messageAcknowledged(MessageSeqId messageId) throws JMSException {

        if (lastUnacknowledged != null && lastUnacknowledged.equals(messageId)) {
            lastUnacknowledged = null;
        }
        switch (hedwigSession.getAcknowledgeMode()) {
        case Session.AUTO_ACKNOWLEDGE:
            undeliveredMessageIds.remove(messageId);
            break;
        case Session.CLIENT_ACKNOWLEDGE:
            unacknowledgedDeliveredMessageIds.remove(messageId);
            break;
        case Session.DUPS_OK_ACKNOWLEDGE:
            undeliveredMessageIds.remove(messageId);
            break;
        case Session.SESSION_TRANSACTED:
            unacknowledgedDeliveredMessageIds.remove(messageId);
            break;
        default:
            break;
        }
    }

    public void offerMessageToSend(Destination destination, HedwigJMSMessage jmsMessage) throws JMSException {
        if (hedwigSession.getTransacted()) {
            pendingMessagesToSend.add(new MessageWithDestination(destination, jmsMessage));
        } else {
            try {
                // messages are sent through a single hedwig client in the
                // session
                // so that they are serially ordered
                hedwigSession
                        .getHedwigProducerForSession()
                        .getPublisher()
                        .publish(ByteString.copyFromUtf8(((Topic) destination).getTopicName()),
                                ((HedwigJMSMessage) jmsMessage).getHedwigMessage());
            } catch (CouldNotConnectException e) {
                JMSUtils.createJMSException("Cannot publish message: cannot connect to broker", e);
            } catch (ServiceDownException e) {
                JMSUtils.createJMSException("Cannot publish message: broker down?", e);
            }
        }
    }

    public boolean offerReceivedMessage(ByteString subscriberId, ByteString topicName,
            org.apache.hedwig.protocol.PubSubProtocol.Message message) throws JMSException {
        lock.lock();
        try {
            HedwigJMSMessage jmsMessage = JMSMessageFactory.getMessage(hedwigSession, subscriberId, message);
            if (jmsMessage.getJMSExpiration() != 0 && (jmsMessage.getJMSExpiration() < System.currentTimeMillis())) {
                Logger.getLogger(SessionMessageQueue.class).info(
                        "JMS message expired and therefore ignored: " + jmsMessage.toString());
                return false;
            }
            if (unacknowledgedDeliveredMessageIds.isEmpty()) {
                recovering = false;
            }
            undeliveredMessageIds.put(message.getMsgId(), new TopicNameAndSubscriberId(topicName, subscriberId));
            if (recovering) {
                if (unacknowledgedDeliveredMessageIds.containsKey(jmsMessage.getMessage().getMsgId())) {
                    jmsMessage.setDelivered();
                }
            }
            boolean result = orderedReceivedMessagesBySubscriber.put(subscriberId, jmsMessage);
            notEmpty.signal();
            subscribersNotEmptyConditions.get(subscriberId).signal();
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void unacknowledgedMessageDelivered(HedwigJMSMessage message) {
        lastUnacknowledged = message.getMessage().getMsgId();
        unacknowledgedDeliveredMessageIds.put(message.getMessage().getMsgId(),
                undeliveredMessageIds.remove(message.getMessage().getMsgId()));
    }

    public HedwigJMSMessage retrieve(ByteString subscriberId, boolean blocking, long time, TimeUnit timeUnit)
            throws JMSException {
        lock.lock();
        try {
            List<HedwigJMSMessage> messages = orderedReceivedMessagesBySubscriber.get(subscriberId);
            if (messages.isEmpty()) {
                try {
                    if (blocking) {
                        subscribersNotEmptyConditions.get(subscriberId).await();
                    } else {
                        subscribersNotEmptyConditions.get(subscriberId).await(time, timeUnit);
                    }

                } catch (InterruptedException e) {
                    // TODO something
                }
            }
            if (!messages.isEmpty()) {
                HedwigJMSMessage next = messages.remove(0);
                if (next.getJMSExpiration() != 0 && next.getJMSExpiration() < System.currentTimeMillis()) {
                    Logger.getLogger(SessionMessageQueue.class).info(
                            "JMS message expired and therefore ignored: " + next.toString());
                    return retrieve(subscriberId, blocking, time, timeUnit);
                }
                return next;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public HedwigJMSMessage blockingRetrieveAny() throws InterruptedException {
        lock.lock();
        try {
            if (orderedReceivedMessagesBySubscriber.isEmpty()) {
                notEmpty.await();
            }
            Collection<HedwigJMSMessage> values = ((Collection<HedwigJMSMessage>) orderedReceivedMessagesBySubscriber
                    .values());
            HedwigJMSMessage next = values.iterator().next();
            values.remove(next);
            return next;
        } finally {
            lock.unlock();
        }
    }

    private static class MessageWithDestination {

        Destination destination;
        HedwigJMSMessage message;

        public MessageWithDestination(Destination destination, HedwigJMSMessage message) {
            super();
            this.destination = destination;
            this.message = message;
        }

        public Destination getDestination() {
            return destination;
        }

        public HedwigJMSMessage getMessage() {
            return message;
        }

    }

    private static class TopicNameAndSubscriberId {
        ByteString topicName;
        ByteString subscriberId;

        public TopicNameAndSubscriberId(ByteString topicName, ByteString subscriberId) {
            super();
            this.topicName = topicName;
            this.subscriberId = subscriberId;
        }

        public ByteString getTopicName() {
            return topicName;
        }

        public ByteString getSubscriberId() {
            return subscriberId;
        }
    }

}
