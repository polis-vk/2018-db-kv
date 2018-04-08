package ru.mail.polis;

import java.util.Arrays;

public class ByteWrapper {
    private final byte[] bytes;

    public ByteWrapper(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteWrapper that = (ByteWrapper) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
     }
}
