package org.apache.hedwig.jms.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.hedwig.jms.administered.HedwigDestination;
import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Key2Boolean;
import org.apache.hedwig.protocol.PubSubProtocol.Key2Double;
import org.apache.hedwig.protocol.PubSubProtocol.Key2Float;
import org.apache.hedwig.protocol.PubSubProtocol.Key2Integer;
import org.apache.hedwig.protocol.PubSubProtocol.Key2Long;
import org.apache.hedwig.protocol.PubSubProtocol.Key2String;
import org.apache.hedwig.protocol.PubSubProtocol.Message.Builder;

import com.google.protobuf.ByteString;

// TODO This class is both read and written. The underlying message is a google protobuf message. 
// A protocol buffer message can only be read once it is built. Once it is built, it cannot be modified.
// Therefore, for now, to support consecutive and mixed reads and writes, we systematically build the underlying 
// protobuf message and create a new builder from that message, for every write operation.
// This surely is inefficient and is a good place to look for performance improvements!
// TODO add checks on property keys
// TODO add checks on property values
// TODO implement all properties, probably using an extra map field in the message structure (for ttl etc...)
public abstract class HedwigJMSMessage implements Message {

    protected Builder builder;
    protected org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage;
    ByteString subscriberId;
    HedwigSession hedwigSession;

    // TODO use a parameter
    public static final int MAX_KRYO_BUFFER_CAPACITY = 16000;
    // modifiable if new message or existing message and cleared body
    boolean modifiableProperties = false;
    protected boolean modifiableBody = false;
    boolean acknowledged = false;
    boolean alreadyDelivered = false;
    private static final ArrayList<String> JMS_HEADER_NAMES = new ArrayList<String>() {
        {
            add("JMSDestination");
            add("JMSDeliveryMode");
            add("JMSExpiration");
            add("JMSPriority");
            add("JMSMessageID");
            add("JMSTimestamp");
            add("JMSCorrelationID");
            add("JMSReplyTo");
            add("JMSType");
            add("JMSRedelivered");
        }
    };

    public HedwigJMSMessage(HedwigSession hedwigSession) {
        builder = org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder();
        this.hedwigSession = hedwigSession;
        hedwigMessage = builder.buildPartial();
        builder = org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder(hedwigMessage);
        modifiableProperties = true;
        modifiableBody = true;
    }

    public HedwigJMSMessage(HedwigSession hedwigSession, ByteString subscriberId,
            org.apache.hedwig.protocol.PubSubProtocol.Message hedwigMessage) {
        this.hedwigSession = hedwigSession;
        this.subscriberId = subscriberId;
        builder = org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder(hedwigMessage);
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

    private void addStringMetadata(String key, String value) {
        List<Key2String> metadataList = builder.getStringMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setStringMetadata(existingKeyIndex, Key2String.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addStringMetadata(Key2String.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
    }

    /**
     * Creates underlying hedwig protobuf message, that can be read, and
     * prepares a new builder from that message.
     */
    protected void buildMessageAndPrepareNewBuilder() {
        hedwigMessage = builder.buildPartial();
        builder = org.apache.hedwig.protocol.PubSubProtocol.Message.newBuilder(hedwigMessage);
    }

    private void addBooleanMetadata(String key, Boolean value) {
        List<Key2Boolean> metadataList = builder.getBooleanMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setBooleanMetadata(existingKeyIndex, Key2Boolean.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addBooleanMetadata(Key2Boolean.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
    }

    // TODO allow byte and integer properties with same name
    private void addIntegerMetadata(String key, Integer value) {
        List<Key2Integer> metadataList = builder.getIntegerMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setIntegerMetadata(existingKeyIndex, Key2Integer.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addIntegerMetadata(Key2Integer.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
    }

    private void addLongMetadata(String key, Long value) {
        List<Key2Long> metadataList = builder.getLongMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setLongMetadata(existingKeyIndex, Key2Long.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addLongMetadata(Key2Long.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
    }

    private void addDoubleMetadata(String key, Double value) {
        List<Key2Double> metadataList = builder.getDoubleMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setDoubleMetadata(existingKeyIndex, Key2Double.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addDoubleMetadata(Key2Double.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
    }

    private void addFloatMetadata(String key, Float value) {
        List<Key2Float> metadataList = builder.getFloatMetadataList();
        int existingKeyIndex = -1;
        for (int i = 0; i < metadataList.size(); i++) {
            if (metadataList.get(i).getKey().equals(key)) {
                existingKeyIndex = i;
                break;
            }
        }
        if (existingKeyIndex != -1) {
            builder.setFloatMetadata(existingKeyIndex, Key2Float.newBuilder().setKey(key).setValue(value).build());
        } else {
            builder.addFloatMetadata(Key2Float.newBuilder().setKey(key).setValue(value).build());
        }
        buildMessageAndPrepareNewBuilder();
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
        modifiableBody = true;
    }

    @Override
    public void clearProperties() throws JMSException {
        builder.clearBooleanMetadata();
        builder.clearDoubleMetadata();
        builder.clearFloatMetadata();
        builder.clearIntegerMetadata();
        builder.clearLongMetadata();
        builder.clearStringMetadata();
        modifiableProperties = true;
    }

    private void checkPropertiesWriteable() throws MessageNotWriteableException {
        if (!modifiableProperties) {
            throw new MessageNotWriteableException(
                    "Properties not writeable. You must clear all properties for enabling modifications");
        }
    }

    protected void checkBodyWriteable() throws MessageNotWriteableException {
        if (!modifiableBody) {
            throw new MessageNotWriteableException(
                    "Body not writeable. You must clear the body for enabling modifications");
        }
    }

    @Override
    public boolean getBooleanProperty(String key) throws JMSException {
        for (Iterator<Key2Boolean> iterator = hedwigMessage.getBooleanMetadataList().iterator(); iterator.hasNext();) {
            Key2Boolean element = (Key2Boolean) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return false;
    }

    @Override
    public byte getByteProperty(String key) throws JMSException {
        // TODO check actually a byte?
        return (byte) getIntProperty(key);
    }

    @Override
    public double getDoubleProperty(String key) throws JMSException {
        for (Iterator<Key2Double> iterator = hedwigMessage.getDoubleMetadataList().iterator(); iterator.hasNext();) {
            Key2Double element = (Key2Double) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return 0;
    }

    @Override
    public float getFloatProperty(String key) throws JMSException {
        for (Iterator<Key2Float> iterator = hedwigMessage.getFloatMetadataList().iterator(); iterator.hasNext();) {
            Key2Float element = (Key2Float) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return 0;
    }

    @Override
    public int getIntProperty(String key) throws JMSException {
        for (Iterator iterator = hedwigMessage.getIntegerMetadataList().iterator(); iterator.hasNext();) {
            Key2Integer element = (Key2Integer) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return 0;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        throw new UnsupportedOperationException("Optional JMS operation not supported by Hedwig");
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        throw new UnsupportedOperationException("Optional JMS operation not supported by Hedwig");
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return getIntProperty("JMSDeliveryMode");
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return getLongProperty("JMSExpiration");
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return getStringProperty("JMSMessageID");
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return getIntProperty("JMSPriority");
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
        return getLongProperty("JMSTimestamp");
    }

    @Override
    public String getJMSType() throws JMSException {
        return getStringProperty("JMSType");
    }

    @Override
    public long getLongProperty(String key) throws JMSException {
        for (Iterator<Key2Long> iterator = hedwigMessage.getLongMetadataList().iterator(); iterator.hasNext();) {
            Key2Long element = (Key2Long) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return 0;
    }

    @Override
    public Object getObjectProperty(String key) throws JMSException {
        // need to iterate all in order to avoid default values
        for (Iterator<Key2Boolean> iterator = hedwigMessage.getBooleanMetadataList().iterator(); iterator.hasNext();) {
            Key2Boolean element = (Key2Boolean) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        for (Iterator<Key2Integer> iterator = hedwigMessage.getIntegerMetadataList().iterator(); iterator.hasNext();) {
            Key2Integer element = (Key2Integer) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        for (Iterator<Key2Long> iterator = hedwigMessage.getLongMetadataList().iterator(); iterator.hasNext();) {
            Key2Long element = (Key2Long) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        for (Iterator<Key2Float> iterator = hedwigMessage.getFloatMetadataList().iterator(); iterator.hasNext();) {
            Key2Float element = (Key2Float) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        for (Iterator<Key2Double> iterator = hedwigMessage.getDoubleMetadataList().iterator(); iterator.hasNext();) {
            Key2Double element = (Key2Double) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        // else check string property, which return null if not found
        return getStringProperty(key);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        List<String> names = listPropertyNames(true);
        return Collections.enumeration(names);
    }

    private List<String> listPropertyNames(boolean excludeJMSStandardHeaders) {
        List<String> names = new ArrayList<String>();
        for (Iterator<Key2Boolean> iterator = hedwigMessage.getBooleanMetadataList().iterator(); iterator.hasNext();) {
            Key2Boolean element = (Key2Boolean) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        for (Iterator<Key2Integer> iterator = hedwigMessage.getIntegerMetadataList().iterator(); iterator.hasNext();) {
            Key2Integer element = (Key2Integer) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        for (Iterator<Key2Long> iterator = hedwigMessage.getLongMetadataList().iterator(); iterator.hasNext();) {
            Key2Long element = (Key2Long) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        for (Iterator<Key2Float> iterator = hedwigMessage.getFloatMetadataList().iterator(); iterator.hasNext();) {
            Key2Float element = (Key2Float) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        for (Iterator<Key2Double> iterator = hedwigMessage.getDoubleMetadataList().iterator(); iterator.hasNext();) {
            Key2Double element = (Key2Double) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        for (Iterator<Key2String> iterator = hedwigMessage.getStringMetadataList().iterator(); iterator.hasNext();) {
            Key2String element = (Key2String) iterator.next();
            if (excludeJMSStandardHeaders) {
                if (!isHeaderField(element.getKey())) {
                    names.add(element.getKey());
                }
            } else {
                names.add(element.getKey());
            }
        }
        return names;
    }

    private boolean isHeaderField(String key) {
        return (JMS_HEADER_NAMES.contains(key));
    }

    @Override
    public short getShortProperty(String key) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getStringProperty(String key) throws JMSException {
        for (Iterator<Key2String> iterator = hedwigMessage.getStringMetadataList().iterator(); iterator.hasNext();) {
            Key2String element = (Key2String) iterator.next();
            if (element.getKey().equals(key)) {
                return element.getValue();
            }
        }
        return null;
    }

    @Override
    public boolean propertyExists(String key) throws JMSException {
        return listPropertyNames(false).contains(key);
    }

    @Override
    public void setBooleanProperty(String key, boolean value) throws JMSException {
        checkPropertiesWriteable();
        addBooleanMetadata(key, value);
    }

    @Override
    public void setByteProperty(String key, byte value) throws JMSException {
        checkPropertiesWriteable();
        addIntegerMetadata(key, (int) value);
    }

    @Override
    public void setDoubleProperty(String key, double value) throws JMSException {
        checkPropertiesWriteable();
        addDoubleMetadata(key, value);
    }

    @Override
    public void setFloatProperty(String key, float value) throws JMSException {
        checkPropertiesWriteable();
        addFloatMetadata(key, value);
    }

    @Override
    public void setIntProperty(String key, int value) throws JMSException {
        checkPropertiesWriteable();
        addIntegerMetadata(key, value);
    }

    @Override
    public void setJMSCorrelationID(String jmsCorrelationID) throws JMSException {
        throw new UnsupportedOperationException("Optional JMS operation not supported by Hedwig");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] id) throws JMSException {
        throw new UnsupportedOperationException("Optional JMS operation not supported by Hedwig");
    }

    @Override
    public void setJMSDeliveryMode(int value) throws JMSException {
        addIntegerMetadata("JMSDeliveryMode", value);
    }

    @Override
    public void setJMSDestination(Destination arg0) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setJMSExpiration(long value) throws JMSException {
        addLongMetadata("JMSExpiration", value);
    }

    @Override
    public void setJMSMessageID(String value) throws JMSException {
        addStringMetadata("JMSMessageID", value);
    }

    @Override
    public void setJMSPriority(int value) throws JMSException {
        addIntegerMetadata("JMSPriority", value);
    }

    @Override
    public void setJMSRedelivered(boolean value) throws JMSException {
        addBooleanMetadata("JMSRedelivered", value);
    }

    @Override
    public void setJMSReplyTo(Destination arg0) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setJMSTimestamp(long value) throws JMSException {
        addLongMetadata("JMSTimestamp", value);

    }

    @Override
    public void setJMSType(String jmsType) throws JMSException {
        addStringMetadata("JMSType", jmsType);
    }

    @Override
    public void setLongProperty(String key, long value) throws JMSException {
        checkPropertiesWriteable();
        addLongMetadata(key, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        checkPropertiesWriteable();
        addIntegerMetadata(name, (int) value);

    }

    @Override
    public void setObjectProperty(String key, Object value) throws JMSException {
        checkPropertiesWriteable();
        if (value instanceof Boolean) {
            addBooleanMetadata(key, (Boolean) value);
        } else if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            addIntegerMetadata(key, (Integer) value);
        } else if (value instanceof Long) {
            addLongMetadata(key, (Long) value);
        } else if (value instanceof Float) {
            addFloatMetadata(key, (Float) value);
        } else if (value instanceof Double) {
            addDoubleMetadata(key, (Double) value);
        } else if (value instanceof String) {
            addStringMetadata(key, (String) value);
        } else {
            throw new MessageFormatException("Cannot add property of type [" + value.getClass().getName() + "]");
        }
    }

    @Override
    public void setStringProperty(String key, String value) throws JMSException {
        checkPropertiesWriteable();
        addStringMetadata(key, value);
    }

    public org.apache.hedwig.protocol.PubSubProtocol.Message getHedwigMessage() {
        return builder.build();
    }

    public boolean isPersistent() {
        // TODO Auto-generated method stub
        return false;
    }

    public HedwigDestination getOriginalJMSDestination() {
        // TODO Auto-generated method stub
        return null;
    }

    public int getRedeliveryCounter() {
        // TODO Auto-generated method stub
        return 0;
    }

    // NOTE: these refer to optional properties, currently not supported in
    // Hedwig
    // public Object getGroupID() {
    // // TODO Auto-generated method stub
    // return null;
    // }
    //
    // public String getGroupSequence() {
    // // TODO Auto-generated method stub
    // return null;
    // }
    //
    // public TransactionId getOriginalTransactionId() {
    // // TODO Auto-generated method stub
    // return null;
    // }
    //
    // public TransactionId getTransactionId() {
    // // TODO Auto-generated method stub
    // return null;
    // }

    public abstract void doPrepareForSend() throws JMSException;

    public abstract JmsBodyType getBodyType();

    public final void prepareForSend() throws JMSException {
        setReadonly();
        doPrepareForSend();
        buildMessageAndPrepareNewBuilder();
        builder.setJmsBodyType(getBodyType());
    }

    private void setReadonly() {
        modifiableBody = false;
        modifiableProperties = false;
    }
}
