package org.apache.hedwig.jms.message;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.MapSerializer;
import com.google.protobuf.ByteString;

public class HedwigJMSMapMessage extends HedwigJMSMessage implements MapMessage {

    Map<String, Object> map = null;

    public HedwigJMSMapMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
        map = new HashMap<String, Object>();
    }

    public HedwigJMSMapMessage(HedwigSession hedwigSession, ByteString subscriberId, Message hedwigMessage) {
        super(hedwigSession, subscriberId, hedwigMessage);
    }

    @Override
    public boolean getBoolean(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Boolean.valueOf((String) value);
            } else {
                return (Boolean) value;
            }
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a boolean : " + e.getMessage());
        }
    }

    private void initializeMap() {
        if (map == null) {
            Kryo kryo = new Kryo();
            kryo.register(HashMap.class, new MapSerializer(kryo));
            map = (Map<String, Object>) kryo.readClassAndObject(hedwigMessage.getBody().asReadOnlyByteBuffer());
        }
    }

    @Override
    public byte getByte(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Byte.valueOf((String) value);
            }
            return (Byte) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a byte : " + e.getMessage());
        }
    }

    @Override
    public byte[] getBytes(String key) throws JMSException {
        initializeMap();
        try {
            return (byte[]) map.get(key);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a byte[] : " + e.getMessage());
        }
    }

    @Override
    public char getChar(String key) throws JMSException {
        initializeMap();
        try {
            if (map.get(key) == null) {
                throw new NullPointerException("Char cannot be null");
            }
            return (Character) map.get(key);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a char : " + e.getMessage());
        }
    }

    @Override
    public double getDouble(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Double.valueOf((String) value);
            }
            return (Double) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a double : " + e.getMessage());
        }
    }

    @Override
    public float getFloat(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Float.valueOf((String) value);
            }
            return (Float) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a float : " + e.getMessage());
        }
    }

    @Override
    public int getInt(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Integer.valueOf((String) value);
            }
            return (Integer) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not an int : " + e.getMessage());
        }
    }

    @Override
    public long getLong(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Long.valueOf((String) value);
            }
            return (Long) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a long : " + e.getMessage());
        }
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        initializeMap();
        return Collections.enumeration(map.keySet());
    }

    @Override
    public Object getObject(String key) throws JMSException {
        initializeMap();
        return map.get(key);
    }

    @Override
    public short getShort(String key) throws JMSException {
        initializeMap();
        try {
            Object value = map.get(key);
            if (value instanceof String) {
                return Short.valueOf((String) value);
            }
            return (Short) value;
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a short: " + e.getMessage());
        }
    }

    @Override
    public String getString(String key) throws JMSException {
        initializeMap();
        try {
            return (String) map.get(key);
        } catch (ClassCastException e) {
            throw new MessageFormatException("Value is not a String : " + e.getMessage());
        }
    }

    @Override
    public boolean itemExists(String key) throws JMSException {
        initializeMap();
        return map.containsKey(key);
    }

    @Override
    public void setBoolean(String key, boolean value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setByte(String key, byte value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setBytes(String key, byte[] value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setBytes(String key, byte[] value, int offset, int length) throws JMSException {
        checkBodyWriteable();
        byte[] data = new byte[length];
        System.arraycopy(value, offset, data, 0, length);
        map.put(key, data);
    }

    @Override
    public void setChar(String key, char value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setDouble(String key, double value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setFloat(String key, float value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setInt(String key, int value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setLong(String key, long value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setObject(String key, Object value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setShort(String key, short value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void setString(String key, String value) throws JMSException {
        checkBodyWriteable();
        map.put(key, value);
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        if (map != null) {
            map.clear();
        } else {
            map = new HashMap<String, Object>();
        }
    }

    @Override
    public void doPrepareForSend() {
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class, new MapSerializer(kryo));
        ObjectBuffer buffer = new ObjectBuffer(kryo, MAX_KRYO_BUFFER_CAPACITY);
        builder.setBody(ByteString.copyFrom(buffer.writeClassAndObject(map)));
    }

    @Override
    public JmsBodyType getBodyType() {
        return JmsBodyType.MAP;
    }
}
