package org.apache.hedwig.jms.administered;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.jms.FileURLHandler;

/**
 * 
 * <u>NOTE:</u> The JMS spec indicate that a Connection encapsulates a
 * connection to a JMS provider, typically a TCP connection.
 * 
 * However, a connection may have multiple subscribers, which receive
 * <b>independent</b> copies of messages, that can be acknowledged
 * independently. In order to satisfy this behaviour, a JMS HedwigConnection
 * object may actually contain <b>several</b> hedwig clients, each maintaining
 * its own connection to the JMS provider.
 * 
 * 
 */
public abstract class HedwigConnection implements Connection {

    // TODO move to a constants class?
    public static final String HEDWIG_CLIENT_CONFIG_FILE = "hedwig.client.config.file";
    // protected HedwigClient hedwigClient;
    boolean isClosed = false;
    ThreadGate startedGate = new ThreadGate();
    Set<HedwigSession> sessions = new HashSet<HedwigSession>();
    ClientConfiguration hedwigClientConfig = new ClientConfiguration();

    public HedwigConnection() {
        try {
            // 1. try to load the client configuration as specified from a
            // system property
            if (System.getProperty(HEDWIG_CLIENT_CONFIG_FILE) != null) {
                File configFile = new File(System.getProperty(HEDWIG_CLIENT_CONFIG_FILE));
                if (!configFile.exists()) {
                    throw new RuntimeException(
                            "Cannot create connection: cannot find Hedwig client configuration file specified as ["
                                    + System.getProperty(HEDWIG_CLIENT_CONFIG_FILE) + "]");
                }
                hedwigClientConfig.loadConf(configFile.toURI().toURL());
            } else {
                // 2. try to load a "hedwig-client.cfg" file from the classpath
                hedwigClientConfig.loadConf(new URL(null, "classpath://hedwig-client.cfg", new FileURLHandler(
                        ClassLoader.getSystemClassLoader())));
            }

        } catch (MalformedURLException e) {
            throw new RuntimeException("Cannot load Hedwig client configuration file", e);
        } catch (org.apache.commons.configuration.ConfigurationException e) {
            throw new RuntimeException("Cannot load Hedwig client configuration", e);
        }
    }

    public void waitUntilStarted() throws InterruptedException {
        startedGate.await();
    }

    private void checkNotClosed() throws IllegalStateException {
        if (isClosed) {
            throw new IllegalStateException("Connection is closed");
        }
    }

    @Override
    public synchronized void close() throws JMSException {
        if (!isClosed) {
            Iterator<HedwigSession> iterator = sessions.iterator();
            while (iterator.hasNext()) {
                HedwigSession next = iterator.next();
                next.close();
            }
            sessions.clear();
            isClosed = true;
        }
    }

    public void registerSession(HedwigSession hedwigSession) {
        sessions.add(hedwigSession);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination arg0, String arg1, ServerSessionPool arg2, int arg3)
            throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic arg0, String arg1, String arg2,
            ServerSessionPool arg3, int arg4) throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkNotClosed();
        if (!transacted) {
            throw new UnsupportedOperationException(
                    "Hedwig's JMS implementation currently only supports transacted sessions");
        }
        HedwigSession session = new HedwigSession(this, acknowledgeMode, hedwigClientConfig);
        return session;

    }

    @Override
    public String getClientID() throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setClientID(String arg0) throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub

    }

    @Override
    public void setExceptionListener(ExceptionListener arg0) throws JMSException {
        checkNotClosed();
        // TODO Auto-generated method stub

    }

    @Override
    public void start() throws JMSException {
        checkNotClosed();
        for (HedwigSession session : sessions) {
            // start delivery of hedwig messages
            session.start();
        }
        // unblock synchronous receivers
        startedGate.open();

    }

    @Override
    public synchronized void stop() throws JMSException {
        checkNotClosed();
        // close delivery of messages already received
        startedGate.close();
        for (HedwigSession session : sessions) {
            // pause reception of messages from the hedwig broker
            session.stop();
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

    public ClientConfiguration getHedwigClientConfig() {
        return hedwigClientConfig;
    }

}
