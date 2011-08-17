package org.apache.hedwig.jms.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;

/**
 * Serializes messages in a session
 * 
 */
public class SessionMessageQueue {

	ListMultimap<ByteString, MessageWithSubscriberId> orderedReceivedMessagesBySubscriber = ArrayListMultimap.create();
	ReentrantLock lock = new ReentrantLock();
	Condition notEmpty = lock.newCondition();
	Map<ByteString, Condition> subscribersNotEmptyConditions = new HashMap<ByteString, Condition>();
	List<MessageWithSubscriberId> unacknowledgedMessages = new ArrayList<MessageWithSubscriberId>();

	// put messages by consumer's client id

	public void addConsumer(ByteString subscriberId) {
		subscribersNotEmptyConditions.put(subscriberId, lock.newCondition());
	}

	public boolean offerReceivedMessage(ByteString subscriberId,
	        org.apache.hedwig.protocol.PubSubProtocol.Message message) {
		lock.lock();
		try {
			boolean result = orderedReceivedMessagesBySubscriber.put(subscriberId, new MessageWithSubscriberId(
			        subscriberId, message));
			notEmpty.signal();
			subscribersNotEmptyConditions.get(subscriberId).signal();
			return result;
		} finally {
			lock.unlock();
		}
	}

	public MessageWithSubscriberId retrieve(ByteString subscriberId, boolean blocking, long time, TimeUnit timeUnit) {
		lock.lock();
		try {
			List<MessageWithSubscriberId> messages = orderedReceivedMessagesBySubscriber.get(subscriberId);
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
				MessageWithSubscriberId next = messages.remove(0);
				unacknowledgedMessages.add(next);
				return next;
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}

	public MessageWithSubscriberId blockingRetrieveAny() {
		lock.lock();
		try {
			ArrayList<MessageWithSubscriberId> values = ((ArrayList<MessageWithSubscriberId>) orderedReceivedMessagesBySubscriber
			        .values());
			if (values.isEmpty()) {
				try {
					notEmpty.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			MessageWithSubscriberId next = values.remove(0);
			unacknowledgedMessages.add(next);
			return next;
		} finally {
			lock.unlock();
		}
	}

	//
	// add blocking methods for adding / removing
	//

	public static class MessageWithSubscriberId {

		ByteString subscriberId;
		org.apache.hedwig.protocol.PubSubProtocol.Message message;

		boolean acknowledged = false;

		public MessageWithSubscriberId(ByteString subscriberId, Message message) {
			super();
			this.subscriberId = subscriberId;
			this.message = message;
		}

		public ByteString getSubscriberId() {
			return subscriberId;
		}

		public org.apache.hedwig.protocol.PubSubProtocol.Message getMessage() {
			return message;
		}

		public void acknowledge() {
			acknowledged = true;
		}

		public boolean isAcknowledged() {
			return acknowledged;
		}

	}

}
