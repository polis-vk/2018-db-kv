package ru.mail.polis.shnus.lsm.sstable.model;

import java.nio.MappedByteBuffer;

public class SSTableLocation {

    private MappedByteBuffer mappedByteBuffer;
    private long fileNumber;
    private long offset;
    private long length;


    public SSTableLocation(MappedByteBuffer mappedByteBuffer, long fileNumber, long offset, long length) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.fileNumber = fileNumber;
        this.offset = offset;
        this.length = length;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public long getFileNumber() {
        return fileNumber;
    }


    public long getOffset() {
        return offset;
    }


    public long getLength() {
        return length;
    }
}
