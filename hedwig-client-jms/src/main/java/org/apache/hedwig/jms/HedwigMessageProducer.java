package org.apache.hedwig.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.client.netty.HedwigPublisher;
import org.apache.hedwig.jms.administered.HedwigSession;

public abstract class HedwigMessageProducer implements MessageProducer {
	
	protected HedwigSession hedwigSession;
	
	public HedwigMessageProducer(HedwigSession hedwigSession) {
		this.hedwigSession = hedwigSession;
	}

	@Override
	public void close() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getDeliveryMode() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Destination getDestination() throws JMSException {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTimeToLive() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void send(Message arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Destination arg0, Message arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Message arg0, int arg1, int arg2, long arg3) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Destination arg0, Message arg1, int arg2, int arg3, long arg4) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDeliveryMode(int arg0) throws JMSException {
		// TODO Auto-generated method stub

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
	public void setPriority(int arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTimeToLive(long arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

}
