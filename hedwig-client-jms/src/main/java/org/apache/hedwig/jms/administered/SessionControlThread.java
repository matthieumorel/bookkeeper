package org.apache.hedwig.jms.administered;

import javax.jms.MessageListener;

import org.apache.hedwig.jms.message.MessageFactory;
import org.apache.hedwig.jms.util.SessionMessageQueue;
import org.apache.hedwig.jms.util.SessionMessageQueue.MessageWithSubscriberId;

public class SessionControlThread extends Thread {

	SessionMessageQueue receivedMessages;
	HedwigSession session;

	public SessionControlThread(SessionMessageQueue receivedMessages, HedwigSession session) {
		this.receivedMessages = receivedMessages;
		this.session = session;
	}

	@Override
	public void run() {

		while (true) {
			try {
				session.getHedwigConnection().waitUntilStarted();
			} catch (InterruptedException ignored) {
			}
			MessageWithSubscriberId next = receivedMessages.blockingRetrieveAny();
			MessageListener messageListener = session.getListeners().get(next.getSubscriberId());
			messageListener.onMessage(MessageFactory.getMessage(next.getMessage()));
		}
	}
}
