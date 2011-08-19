package org.apache.hedwig.jms.message;

import org.apache.hedwig.jms.administered.HedwigSession;

import com.google.protobuf.ByteString;

public class JMSMessageFactory {

	public static HedwigJMSMessage getMessage(HedwigSession hedwigSession, ByteString subscriberId,
	        org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage) {
		// TODO identify message type according to metadata
		return new HedwigJMSTextMessage(hedwigSession, subscriberId, hedwigMessage);
	}

}
