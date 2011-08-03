package org.apache.hedwig.jms.administered;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.netty.HedwigClient;
import org.apache.hedwig.jms.HedwigMessageConsumer;

public abstract class HedwigConnection implements Connection {

	protected HedwigClient hedwigClient;
	boolean started;
	List<HedwigMessageConsumer> consumers = new ArrayList<HedwigMessageConsumer>();
	ThreadGate startedGate = new ThreadGate();	

	public HedwigConnection() {
		ClientConfiguration config = new ClientConfiguration();
		try {
			config.loadConf(new URL(null, "classpath://hedwig-client.cfg", new Handler(ClassLoader
			        .getSystemClassLoader())));
			this.hedwigClient = new HedwigClient(config);
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HedwigClient getHedwigClient() {
		return hedwigClient;
	}

	public void waitUntilStarted() throws InterruptedException {
		startedGate.await();
	}
	
	@Override
	public void close() throws JMSException {
//		startedGate.close();
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
		if (acknowledgeMode != Session.CLIENT_ACKNOWLEDGE) {
			throw new UnsupportedOperationException(
			        "Hedwig's JMS implementation currently only supports client acknowledged sessions");
		}
		return new HedwigSession(this);
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

	public class Handler extends URLStreamHandler {
		/** The classloader to find resources from. */
		private final ClassLoader classLoader;

		public Handler(ClassLoader classLoader) {
			this.classLoader = classLoader;
		}

		@Override
		protected URLConnection openConnection(URL u) throws IOException {
			final URL resourceUrl = classLoader.getResource(u.getPath());
			return resourceUrl.openConnection();
		}
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
