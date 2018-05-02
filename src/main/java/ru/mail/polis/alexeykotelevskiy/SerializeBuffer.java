package ru.mail.polis.alexeykotelevskiy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public class SerializeBuffer implements Externalizable, Comparable<SerializeBuffer> {
    private transient ByteBuffer buff;

    public ByteBuffer getBuff() {
        return buff;
    }

    public SerializeBuffer() {
    }

    SerializeBuffer(byte[] a) {
        buff = ByteBuffer.wrap(a);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(buff.array());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        buff = ByteBuffer.wrap((byte[]) in.readObject());
    }

    @Override
    public int compareTo(@NotNull SerializeBuffer o) {
        return buff.compareTo(o.getBuff());
    }
}
