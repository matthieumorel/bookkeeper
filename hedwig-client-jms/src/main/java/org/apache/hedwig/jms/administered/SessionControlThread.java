package org.apache.hedwig.jms.administered;

import javax.jms.JMSException;
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
        setName("HedwigSessionControlThread");
    }

    @Override
    public void run() {

        while (true) {
            try {
                session.getHedwigConnection().waitUntilStarted();
            } catch (InterruptedException ignored) {
            }
            try {
                HedwigJMSMessage next;
                try {
                    next = receivedMessages.blockingRetrieveAny();
                } catch (InterruptedException e) {
                    // session is closing
                    return;
                }
                MessageListener messageListener = session.getListeners().get(next.getSubscriberId());
                messageListener.onMessage(JMSMessageFactory.getMessage(session, next.getSubscriberId(),
                        next.getMessage()));
                session.messageSuccessfullyDelivered(next);
            } catch (RuntimeException e) {
                // TODO just log it
                e.printStackTrace();
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
