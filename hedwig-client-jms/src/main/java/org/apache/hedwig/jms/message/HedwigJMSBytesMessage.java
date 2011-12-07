package org.apache.hedwig.jms.message;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.util.JMSUtils;
import org.apache.hedwig.protocol.PubSubProtocol.JmsBodyType;
import org.apache.hedwig.protocol.PubSubProtocol.Message;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

public class HedwigJMSBytesMessage extends HedwigJMSMessage implements BytesMessage {

    ByteArrayDataInput input;
    ByteArrayDataOutput output;

    public HedwigJMSBytesMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
        output = ByteStreams.newDataOutput();
    }

    public HedwigJMSBytesMessage(HedwigSession hedwigSession, ByteString subscriberId, Message hedwigMessage) {
        super(hedwigSession, subscriberId, hedwigMessage);
    }

    private void checkReadOnlyMode() throws MessageNotReadableException {
        if (modifiableBody) {
            throw new MessageNotReadableException("Message not in read-only mode. Use reset() to switch mode");
        }
        initializeInput();
    }

    private void initializeInput() {
        if (input == null) {
            input = ByteStreams.newDataInput(hedwigMessage.getBody().toByteArray());
        }
    }

    private void checkWriteOnlyMode() throws MessageNotWriteableException {
        checkBodyWriteable();
    }

    @Override
    public long getBodyLength() throws JMSException {
        return hedwigMessage.getBody().size();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readBoolean();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read boolean from bytes message", e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readByte();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read byte from bytes message", e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readUnsignedByte();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read unsigned byte from bytes message", e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readShort();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read short from bytes message", e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readUnsignedShort();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read unsigned short from bytes message", e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readChar();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read char from bytes message", e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readInt();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read int from bytes message", e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readLong();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read long from bytes message", e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readFloat();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read float from bytes message", e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readDouble();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read double from bytes message", e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        checkReadOnlyMode();
        try {
            return input.readUTF();
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read UTF from bytes message", e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        checkReadOnlyMode();
        try {
            input.readFully(value);
            return value.length;
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read bytes from bytes message", e);
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        checkReadOnlyMode();
        try {
            input.readFully(value, 0, length);
            return length;
        } catch (IllegalStateException e) {
            throw JMSUtils.createJMSException("Cannot read bytes from bytes message", e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkWriteOnlyMode();
        output.writeBoolean(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkWriteOnlyMode();
        output.writeByte(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkWriteOnlyMode();
        output.writeShort(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkWriteOnlyMode();
        output.writeChar(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkWriteOnlyMode();
        output.writeInt(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkWriteOnlyMode();
        output.writeLong(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkWriteOnlyMode();
        output.writeFloat(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkWriteOnlyMode();
        output.writeDouble(value);
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        checkWriteOnlyMode();
        output.writeUTF(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkWriteOnlyMode();
        output.write(value);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkWriteOnlyMode();
        output.write(value, offset, length);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        checkWriteOnlyMode();
        if (value == null) {
            throw new NullPointerException("Null parameter");
        }
        if (value instanceof Boolean) {
            writeBoolean((Boolean) value);
        } else if (value instanceof Character) {
            writeChar((Character) value);
        } else if (value instanceof Byte) {
            writeByte(((Byte) value));
        } else if (value instanceof Short) {
            writeShort(((Short) value));
        } else if (value instanceof Integer) {
            writeInt(((Integer) value));
        } else if (value instanceof Double) {
            writeDouble(((Double) value));
        } else if (value instanceof Long) {
            writeLong(((Long) value));
        } else if (value instanceof Float) {
            writeFloat(((Float) value));
        } else if (value instanceof Double) {
            writeDouble(((Double) value));
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Invalid type [" + value.getClass().getName()
                    + "] : object must be a primitive object type, a String or a byte[]");
        }
    }

    @Override
    public void reset() throws JMSException {
        input = ByteStreams.newDataInput(hedwigMessage.getBody().toByteArray());
        modifiableBody = false;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        output = ByteStreams.newDataOutput();
    }

    @Override
    public void doPrepareForSend() {
        builder.setBody(ByteString.copyFrom(output.toByteArray()));
    }

    @Override
    public JmsBodyType getBodyType() {
        return JmsBodyType.BYTES;
    }

}
