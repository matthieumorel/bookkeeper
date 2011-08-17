package org.apache.hedwig.jms;

import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.client.netty.HedwigPublisher;
import org.apache.hedwig.jms.administered.HedwigSession;

public abstract class HedwigMessageProducer implements MessageProducer {
	
	protected HedwigSession hedwigSession;
	private HedwigClient hedwigClient;
	
	public HedwigMessageProducer(HedwigSession hedwigSession) {
		this.hedwigSession = hedwigSession;
		ClientConfiguration config = new ClientConfiguration();
		try {
			config.loadConf(new URL(null, "classpath://hedwig-client.cfg", new FileURLHandler(ClassLoader
			        .getSystemClassLoader())));
			this.hedwigClient = new HedwigClient(config);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (org.apache.commons.configuration.ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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

	public HedwigClient getHedwigClient() {
	    return hedwigClient;
    }

}
