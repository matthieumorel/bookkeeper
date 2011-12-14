package org.apache.hedwig.jms.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.SerializationException;
import com.esotericsoftware.kryo.serialize.ArraySerializer;
import com.esotericsoftware.kryo.serialize.CollectionSerializer;
import com.google.protobuf.ByteString;

/**
 * An implementation backed by a {@link Deque<Object>} which is serialized and
 * put in message body before send
 * 
 */
public class HedwigJMSStreamMessage extends HedwigJMSMessage implements StreamMessage {

    Queue<Object> stream = null;

    public HedwigJMSStreamMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
    }

    public HedwigJMSStreamMessage(HedwigSession hedwigSession, ByteString subscriberId, Message hedwigMessage) {
        super(hedwigSession, subscriberId, hedwigMessage);
    }

    private void checkReadOnlyMode() throws MessageNotReadableException {
        if (modifiableBody) {
            throw new MessageNotReadableException("Message not in read-only mode. Use reset() to switch mode");
        }
    }

    private void checkWriteOnlyMode() throws MessageNotWriteableException {
        checkBodyWriteable();
        if (stream == null) {
            stream = new LinkedList<Object>();
        }
    }

    private void initializeStream() {
        if (stream == null) {
            Kryo kryo = new Kryo();
            kryo.register(LinkedList.class, new CollectionSerializer(kryo));
            kryo.register(byte[].class, new ArraySerializer(kryo));
            stream = (Deque<Object>) kryo.readClassAndObject(hedwigMessage.getBody().asReadOnlyByteBuffer());
        }
    }

    protected Object getNextValueFromStream() throws MessageEOFException, MessageNotReadableException {
        checkReadOnlyMode();
        initializeStream();
        if (stream.isEmpty()) {
            throw new MessageEOFException("No more elements");
        }
        Object value = stream.poll();
        return value;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Boolean.valueOf((String) value));
        }
        try {
            return (Boolean) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Boolean as expected");
        }
    }

    @Override
    public byte readByte() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Byte.valueOf((String) value));
        }
        try {
            return (Byte) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Byte as expected");
        }
    }

    @Override
    public int readBytes(byte[] buffer) throws JMSException {
        Object value = getNextValueFromStream();
        if (value == null) {
            return -1;
        }
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream((byte[]) value);
            try {
                return bais.read(buffer);
            } catch (IOException e) {
                throw new JMSException("Cannot read contents of byte[]: " + e.getMessage());
            } finally {
                try {
                    bais.close();
                } catch (IOException ignored) {
                    // ignored
                }
            }
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a byte[] as expected");
        }
    }

    @Override
    public char readChar() throws JMSException {
        Object value = getNextValueFromStream();
        try {
            return (Character) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Character as expected");
        }
    }

    @Override
    public double readDouble() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Double.valueOf((String) value));
        }
        try {
            return (Double) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Double as expected");
        }
    }

    @Override
    public float readFloat() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Float.valueOf((String) value));
        }
        try {
            return (Float) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Float as expected");
        }
    }

    @Override
    public int readInt() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Integer.valueOf((String) value));
        }
        try {
            return (Integer) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not an Integer as expected");
        }
    }

    @Override
    public long readLong() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Long.valueOf((String) value));
        }
        try {
            return (Long) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Long as expected");
        }
    }

    @Override
    public Object readObject() throws JMSException {
        Object value = getNextValueFromStream();
        return value;
    }

    @Override
    public short readShort() throws JMSException {
        Object value = getNextValueFromStream();
        if (value instanceof String) {
            return (Short.valueOf((String) value));
        }
        try {
            return (Short) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a Short as expected");
        }
    }

    @Override
    public String readString() throws JMSException {
        Object value = getNextValueFromStream();
        try {
            return (String) (value);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is of type [" + value.getClass().getName()
                    + "] and not a String as expected");
        }
    }

    @Override
    public void reset() throws JMSException {
        stream = null;
        initializeStream();
        modifiableBody = false;
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkWriteOnlyMode();
        byte[] out = new byte[length];
        System.arraycopy(value, offset, out, 0, length);
        stream.add(out);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void writeString(String value) throws JMSException {
        checkWriteOnlyMode();
        stream.add(value);
    }

    @Override
    public void doPrepareForSend() throws JMSException {
        if (stream == null) {
            stream = new LinkedList<Object>();
        }
        Kryo kryo = new Kryo();
        kryo.register(LinkedList.class, new CollectionSerializer(kryo));
        kryo.register(byte[].class, new ArraySerializer(kryo));
        ObjectBuffer buffer = new ObjectBuffer(kryo, MAX_KRYO_BUFFER_CAPACITY);
        try {
            builder.setBody(ByteString.copyFrom(buffer.writeClassAndObject(stream)));
        } catch (SerializationException e) {
            throw new JMSException("Cannot write content of message: " + e.getMessage());
        }
    }

    @Override
    public JmsBodyType getBodyType() {
        return JmsBodyType.STREAM;
    }

}
