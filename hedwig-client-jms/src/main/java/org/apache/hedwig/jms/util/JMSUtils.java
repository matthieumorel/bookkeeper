package org.apache.hedwig.jms.util;

import javax.jms.JMSException;

public class JMSUtils {
	
	public static JMSException createJMSException(String reason, Exception linkedException) {
		JMSException e = new JMSException(reason);
		e.setLinkedException(linkedException);
		return e;
	}
	

}
