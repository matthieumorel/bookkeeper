package org.apache.hedwig.jms.administered;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

public class HedwigConnectionFactory implements ConnectionFactory, TopicConnectionFactory {
	
	public HedwigConnectionFactory() {
		
	}

	@Override
	public Connection createConnection() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection createConnection(String arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TopicConnection createTopicConnection() throws JMSException {
		return new HedwigTopicConnection();
	}

	@Override
	public TopicConnection createTopicConnection(String arg0, String arg1)
			throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

}
