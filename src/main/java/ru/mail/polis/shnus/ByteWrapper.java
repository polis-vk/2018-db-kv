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
    public int compareTo(@NotNull Object o) {

        byte[] o1 = getBytes();
        byte[] o2 = ((ByteWrapper) o).getBytes();
        if (o1.length > o2.length) {
            return 1;
        }
        if (o1.length < o2.length) {
            return -1;
        }
        for (int i = 0; i < o1.length; i++) {
            if (o1[i] > o2[i]) {
                return 1;
            }

            if (o1[i] < o2[i]) {
                return -1;
            }
        }

        return 0;
    }


}
