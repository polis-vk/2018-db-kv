package ru.mail.polis.shnus.lsm.sstable.services;

import java.nio.channels.FileChannel;

public class SSTableLocation {
    private FileChannel fileChannel;
    private long fileNumber;
    private long offset;
    private long length;


    public SSTableLocation(FileChannel fileChannel, long fileNumber, long offset, long length){
        this.fileChannel = fileChannel;
        this.fileNumber = fileNumber;
        this.offset = offset;
        this.length = length;
    }


    public FileChannel getFileChannel() {
        return fileChannel;
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
