package ru.mail.polis.shnus;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ByteWrapper implements Comparable {
    final private byte[] data;

    public ByteWrapper(byte[] data) {
        this.data = data;
    }

    public byte[] getBytes() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteWrapper that = (ByteWrapper) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(@NotNull Object o) { //to long? no ideas
        String o1 = Arrays.toString(data);
        String o2 = Arrays.toString(((ByteWrapper) o).getBytes());
        return o1.compareTo(o2);
    }
}
