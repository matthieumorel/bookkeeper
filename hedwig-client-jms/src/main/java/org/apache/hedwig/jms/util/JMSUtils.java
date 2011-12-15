package org.apache.hedwig.jms.util;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.hedwig.jms.administered.HedwigConnection;
import org.apache.hedwig.jms.administered.HedwigTemporaryTopic;
import org.apache.hedwig.jms.administered.HedwigTopic;

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

    public static Destination createDestinationFromDestinationString(String destinationString,
            HedwigConnection connection) throws JMSException {
        if (destinationString.startsWith("topic-")) {
            return new HedwigTopic(destinationString.substring("topic-".length()));
        } else if (destinationString.startsWith("temporaryTopic-")) {
            return new HedwigTemporaryTopic(connection, destinationString.substring("temporaryTopic-".length()));
        } else {
            throw new JMSException("Cannot understand destination " + destinationString);
        }
    }

}
