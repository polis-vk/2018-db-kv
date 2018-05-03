package ru.mail.polis.shnus;

import java.util.Arrays;

public class ByteWrapper {
    final private byte[] data;

    public ByteWrapper(byte[] data) {
        this.data = data;
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
}
