package ru.mail.polis.shnus.lsm.sstable.model;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.shnus.ByteWrapper;

public class KeyAndOffset implements Comparable<KeyAndOffset> {

    private ByteWrapper key;
    private long offset;
    private long length;

    public KeyAndOffset(ByteWrapper key, long offset, long length) {
        this.key = key;
        this.offset = offset;
        this.length = length;
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

    @Override
    public int compareTo(@NotNull KeyAndOffset o) {
        return key.compareTo(o.key);
    }
}