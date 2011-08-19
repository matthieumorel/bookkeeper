package org.apache.hedwig.jms.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.JMSException;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.message.JMSMessageFactory;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;

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
	Set<MessageSeqId> unacknowledgedDeliveredMessageIds = new HashSet<PubSubProtocol.MessageSeqId>();
	boolean recovering = false;
	HedwigSession hedwigSession;

	// put messages by consumer's client id

	public SessionMessageQueue(HedwigSession hedwigSession) {
		this.hedwigSession = hedwigSession;
	}

	public void addConsumer(ByteString subscriberId) {
		subscribersNotEmptyConditions.put(subscriberId, lock.newCondition());
	}

	public void recoveryInitiated() {
		recovering = true;
	}

	public boolean offerReceivedMessage(ByteString subscriberId,
	        org.apache.hedwig.protocol.PubSubProtocol.Message message) {
		lock.lock();
		try {
			HedwigJMSMessage jmsMessage = JMSMessageFactory.getMessage(hedwigSession, subscriberId, message);
			if (recovering) {
				if (unacknowledgedDeliveredMessageIds.contains(jmsMessage.getMessage().getMsgId())) {
					jmsMessage.setDelivered();
					unacknowledgedDeliveredMessageIds.remove(jmsMessage.getMessage().getMsgId());
					if (unacknowledgedDeliveredMessageIds.isEmpty()) {
						recovering = false;
					}
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
		unacknowledgedDeliveredMessageIds.add(message.getMessage().getMsgId());
	}

	public HedwigJMSMessage retrieve(ByteString subscriberId, boolean blocking, long time, TimeUnit timeUnit) {
		lock.lock();
		try {
			List<HedwigJMSMessage> messages = orderedReceivedMessagesBySubscriber.get(subscriberId);
			boolean exists = false;
			if (messages.isEmpty()) {
				try {
					if (blocking) {
						subscribersNotEmptyConditions.get(subscriberId).await();
						exists = true;
					} else {
						exists = subscribersNotEmptyConditions.get(subscriberId).await(time, timeUnit);
					}

				} catch (InterruptedException e) {
					// TODO something
				}
			} else {
				exists = true;
			}
			if (exists) {
				HedwigJMSMessage next = messages.remove(0);
				try {
					System.out.println("** returning " + next.getMessage().getMsgId() + " with delivered = "
					        + next.getJMSRedelivered());
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return next;
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}

	public HedwigJMSMessage blockingRetrieveAny() {
		lock.lock();
		try {
			ArrayList<HedwigJMSMessage> values = ((ArrayList<HedwigJMSMessage>) orderedReceivedMessagesBySubscriber
			        .values());
			if (values.isEmpty()) {
				try {
					notEmpty.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			HedwigJMSMessage next = values.remove(0);
			return next;
		} finally {
			lock.unlock();
		}
	}

}
