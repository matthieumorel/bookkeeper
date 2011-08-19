package org.apache.hedwig.jms.administered;

import javax.jms.MessageListener;

import org.apache.hedwig.jms.message.HedwigJMSMessage;
import org.apache.hedwig.jms.message.JMSMessageFactory;
import org.apache.hedwig.jms.util.SessionMessageQueue;

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
			try {
				HedwigJMSMessage next = receivedMessages.blockingRetrieveAny();
				MessageListener messageListener = session.getListeners().get(next.getSubscriberId());
				messageListener.onMessage(JMSMessageFactory.getMessage(session, next.getSubscriberId(),
				        next.getMessage()));
				session.messageSuccessfullyDelivered(next);
			} catch (RuntimeException e) {
				// TODO just log it
				e.printStackTrace();
			}
		}
	}
}
