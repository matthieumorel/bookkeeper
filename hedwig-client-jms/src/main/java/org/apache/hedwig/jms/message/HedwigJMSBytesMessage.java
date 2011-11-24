package org.apache.hedwig.jms.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.hedwig.jms.administered.HedwigSession;
import org.apache.hedwig.jms.util.JMSUtils;

public class HedwigJMSBytesMessage extends HedwigJMSMessage implements BytesMessage {

    boolean readOnlyMode = false;
    DataInputStream inputByteBuffer;
    DataOutputStream outputByteBuffer;

    // DataOutputStream

    public HedwigJMSBytesMessage(HedwigSession hedwigSession) {
        super(hedwigSession);
        // TODO Auto-generated constructor stub
    }

    private void checkReadOnlyMode() throws MessageNotReadableException {
        if (!readOnlyMode) {
            throw new MessageNotReadableException("Message not in read-only mode. Use reset() to switch mode");
        }
    }

    private void checkWriteOnlyMode() throws MessageNotWriteableException {
        if (readOnlyMode) {
            throw new MessageNotWriteableException("Message not in write-only mode. Use clearBody() to switch mode");
        }
    }

    @Override
    public long getBodyLength() throws JMSException {
        return hedwigMessage.getBody().size();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readBoolean();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read boolean from bytes message", e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readByte();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read byte from bytes message", e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readUnsignedByte();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read unsigned byte from bytes message", e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readShort();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read short from bytes message", e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readUnsignedShort();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read unsigned short from bytes message", e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readChar();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read char from bytes message", e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readInt();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read int from bytes message", e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readLong();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read long from bytes message", e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readFloat();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read float from bytes message", e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readDouble();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read double from bytes message", e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.readUTF();
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read UTF from bytes message", e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        checkReadOnlyMode();
        try {
            return inputByteBuffer.read(value);
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read bytes from bytes message", e);
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        checkReadOnlyMode();
        try {
            // TODO check compatibility
            return inputByteBuffer.read(value, 0, length);
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot read bytes from bytes message", e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkWriteOnlyMode();
        try {
            outputByteBuffer.writeBoolean(value);
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot write boolean to bytes message", e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkWriteOnlyMode();
        try {
            outputByteBuffer.writeByte(value);
        } catch (IOException e) {
            throw JMSUtils.createJMSException("Cannot write byte to bytes message", e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeChar(char value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeInt(int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeLong(long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeFloat(float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeDouble(double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeUTF(String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeObject(Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void reset() throws JMSException {
        inputByteBuffer = new DataInputStream(new ByteArrayInputStream(hedwigMessage.getBody().toByteArray()));
        readOnlyMode = true;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        outputByteBuffer = new DataOutputStream(new ByteArrayOutputStream());
        readOnlyMode = false;
    }

}
