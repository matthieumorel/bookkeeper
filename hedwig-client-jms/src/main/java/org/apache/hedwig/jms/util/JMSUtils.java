package org.apache.hedwig.jms.util;

import javax.jms.JMSException;

public class JMSUtils {

    public static JMSException createJMSException(String reason, Exception linkedException) {
        JMSException e = new JMSException(reason);
        e.setLinkedException(linkedException);
        return e;
    }

    public static JMSException createJMSException(Exception linkedException) {
        JMSException e = new JMSException(linkedException.getMessage());
        e.setLinkedException(linkedException);
        return e;
    }

}
