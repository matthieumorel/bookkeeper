package org.apache.hedwig.jms.administered;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.hedwig.jms.HedwigMessageConsumer;

/**
 * 
 * <u>NOTE:</u> The JMS spec indicate that a Connection encapsulates a
 * connection to a JMS provider, typically a TCP connection.
 * 
 * However, a connection may have multiple subscribers, which receive
 * <b>independent</b> copies of messages, that can be acknowledged
 * independently. In order to satisfy this behaviour, a JMS HedwigConnection
 * object may actually contain <b>several</b> hedwig clients, each maintaining
 * its connection to the JMS provider.
 * 
 * 
 */
public abstract class HedwigConnection implements Connection {

	// protected HedwigClient hedwigClient;
	boolean started;
	List<HedwigMessageConsumer> consumers = new ArrayList<HedwigMessageConsumer>();
	ThreadGate startedGate = new ThreadGate();

	public HedwigConnection() {
	}

	public void waitUntilStarted() throws InterruptedException {
		startedGate.await();
	}

	@Override
	public void close() throws JMSException {
	}

	public void addMessageConsumer(HedwigMessageConsumer consumer) {
		consumers.add(consumer);
	}

	@Override
	public ConnectionConsumer createConnectionConsumer(Destination arg0, String arg1, ServerSessionPool arg2, int arg3)
	        throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectionConsumer createDurableConnectionConsumer(Topic arg0, String arg1, String arg2,
	        ServerSessionPool arg3, int arg4) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
		if (!transacted) {
			throw new UnsupportedOperationException(
			        "Hedwig's JMS implementation currently only supports transacted sessions");
		}
		return new HedwigSession(this, acknowledgeMode);
	}

	@Override
	public String getClientID() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExceptionListener getExceptionListener() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectionMetaData getMetaData() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setClientID(String arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setExceptionListener(ExceptionListener arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void start() throws JMSException {
		startedGate.open();
	}

	@Override
	public synchronized void stop() throws JMSException {
		startedGate.close();
	}

	// from "java concurrency in practice"...
	private class ThreadGate {
		private boolean isOpen;
		private int generation;

		public synchronized void close() {
			isOpen = false;
		}

		public synchronized void open() {
			++generation;
			isOpen = true;
			notifyAll();
		}

		public synchronized void await() throws InterruptedException {
			int arrivalGeneration = generation;
			while (!isOpen && arrivalGeneration == generation) {
				wait();
			}
		}
	}
}
