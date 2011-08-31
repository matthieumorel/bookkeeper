package org.apache.hedwig.jms.message;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.Message.Builder;

import com.google.protobuf.ByteString;

// TODO implement all properties, probably using an extra map field in the message structure (for ttl etc...)
public class HedwigJMSMessage implements Message {

	protected Builder builder;
	protected org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage;
	ByteString subscriberId;
	HedwigSession hedwigSession;

	boolean acknowledged = false;
	boolean alreadyDelivered = false;

	public HedwigJMSMessage(HedwigSession hedwigSession) {
		builder = org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder();
		this.hedwigSession = hedwigSession;
	}

	public HedwigJMSMessage(HedwigSession hedwigSession, ByteString subscriberId,
	        org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage) {
		this.hedwigSession = hedwigSession;
		this.subscriberId = subscriberId;
		this.hedwigMessage = hedwigMessage;
	}

	public ByteString getSubscriberId() {
		return subscriberId;
	}

	public org.apache.hedwig.protocol.PubSubProtocol.Message getMessage() {
		return hedwigMessage;
	}

	public boolean isAcknowledged() {
		return acknowledged;
	}

	public void setDelivered() {
		this.alreadyDelivered = true;
	}

	public HedwigSession getHedwigSession() {
		return hedwigSession;
	}

	@Override
	public void acknowledge() throws JMSException {
		if (hedwigSession.isClosed()) {
			throw new IllegalStateException("Session is closed");
		}
		hedwigSession.getConsumer(subscriberId).acknowledge(getMessage().getMsgId());

	}

	@Override
	public void clearBody() throws JMSException {
		builder.clearBody();

	}

	@Override
	public void clearProperties() throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean getBooleanProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByteProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDoubleProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloatProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getIntProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getJMSCorrelationID() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getJMSDeliveryMode() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Destination getJMSDestination() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getJMSExpiration() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getJMSMessageID() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getJMSPriority() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getJMSRedelivered() throws JMSException {
		return alreadyDelivered;
	}

	@Override
	public Destination getJMSReplyTo() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getJMSTimestamp() throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getJMSType() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLongProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object getObjectProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Enumeration getPropertyNames() throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShortProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getStringProperty(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean propertyExists(String arg0) throws JMSException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setBooleanProperty(String arg0, boolean arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setByteProperty(String arg0, byte arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setDoubleProperty(String arg0, double arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setFloatProperty(String arg0, float arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setIntProperty(String arg0, int arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSCorrelationID(String arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSCorrelationIDAsBytes(byte[] arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSDeliveryMode(int arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSDestination(Destination arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSExpiration(long arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSMessageID(String arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSPriority(int arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSRedelivered(boolean arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSReplyTo(Destination arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSTimestamp(long arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setJMSType(String arg0) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setLongProperty(String arg0, long arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObjectProperty(String arg0, Object arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setShortProperty(String arg0, short arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStringProperty(String arg0, String arg1) throws JMSException {
		// TODO Auto-generated method stub

	}

	public org.apache.hedwig.protocol.PubSubProtocol.Message getHedwigMessage() {
		return builder.build();
	}

}