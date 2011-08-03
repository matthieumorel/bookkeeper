package org.apache.hedwig.jms;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.jms.administered.HedwigConnection;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.message.MessageFactory;
import org.apache.hedwig.jms.util.ClientIdGenerator;
import org.apache.hedwig.jms.util.JMSUtils;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

public class HedwigMessageConsumer implements MessageConsumer, MessageHandler {

	protected ByteString topicName;
	protected ByteString subscriberId;
	protected BlockingQueue<org.apache.hedwig.protocol.PubSubProtocol.Message> unAcknowledgedHedwigMessages = new LinkedBlockingQueue<org.apache.hedwig.protocol.PubSubProtocol.Message>();
	protected HedwigSession hedwigSession;
	Lock connectionStateLock  = new ReentrantLock();
	Condition started = connectionStateLock.newCondition();

	public HedwigMessageConsumer(HedwigSession session, ByteString topicName) {
		this.topicName = topicName;
		this.hedwigSession = session;
		this.subscriberId = ByteString.copyFromUtf8(ClientIdGenerator.getNewClientId());
		try {
			session.getHedwigConnection().getHedwigClient().getSubscriber()
			        .subscribe(topicName, subscriberId, CreateOrAttach.CREATE_OR_ATTACH);
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

	@Override
	public String getMessageSelector() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMessageListener(MessageListener listener) throws JMSException {
		// TODO Auto-generated method stub

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
		try {
	        hedwigSession.getHedwigConnection().waitUntilStarted();
        } catch (InterruptedException e1) {
	        // TODO Auto-generated catch block
	        e1.printStackTrace();
        }

		try {
			hedwigSession.getHedwigConnection().getHedwigClient().getSubscriber()
			        .startDelivery(topicName, subscriberId, this);
		} catch (ClientNotSubscribedException e) {
			throw JMSUtils.createJMSException("Cannot receive message", e);
		}
		try {
			if (timeout == 0) {
				org.apache.hedwig.protocol.PubSubProtocol.Message hMessage = unAcknowledgedHedwigMessages.take();
				return MessageFactory.getMessage(hMessage);
			} else if (timeout > 0) {
				org.apache.hedwig.protocol.PubSubProtocol.Message hMessage = unAcknowledgedHedwigMessages.poll(timeout,
				        TimeUnit.MILLISECONDS);
				return MessageFactory.getMessage(hMessage);
			} else {
				org.apache.hedwig.protocol.PubSubProtocol.Message hMessage = unAcknowledgedHedwigMessages.poll();
				return MessageFactory.getMessage(hMessage);
			}
		} catch (InterruptedException e) {
			throw JMSUtils.createJMSException("Cannot receive message", e);
		}
	}

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized void consume(ByteString topic, ByteString subscriberId,
	        org.apache.hedwig.protocol.PubSubProtocol.Message msg, Callback<Void> callback, Object context) {
		if (this.topicName.equals(topic) && this.subscriberId.equals(subscriberId)) {
			System.out.println("--> " + msg.getBody().toStringUtf8());
			unAcknowledgedHedwigMessages.offer(msg);
		}

	}

	public void signalStarted() {
	    started.signal();
    }
	
	public void signalStopped() {
		connectionStateLock.lock();
	}

}
