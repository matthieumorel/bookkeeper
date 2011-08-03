package org.apache.hedwig.jms.message;

public class MessageFactory {
	
	public static HedwigJMSMessage getMessage(org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage) {
		return new HedwigJMSTextMessage(hedwigMessage);
	}

}
