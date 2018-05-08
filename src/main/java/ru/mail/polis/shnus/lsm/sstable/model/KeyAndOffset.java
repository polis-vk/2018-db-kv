package ru.mail.polis.shnus.lsm.sstable.model;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.shnus.ByteWrapper;

public class KeyAndOffset implements Comparable<KeyAndOffset> {

    private ByteWrapper key;
    private long offset;
    private long length;
    private long indexPosition;

    public KeyAndOffset(ByteWrapper key, long offset, long length, long indexPosition) {
        this.key = key;
        this.offset = offset;
        this.length = length;
        this.indexPosition = indexPosition;
    }

    public ByteWrapper getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long getIndexPosition() {
        return indexPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyAndOffset that = (KeyAndOffset) o;

        return key != null ? key.equals(that.key) : that.key == null;
    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }

    @Override
    public int compareTo(@NotNull KeyAndOffset o) {
        return key.compareTo(o.key);
    }

}