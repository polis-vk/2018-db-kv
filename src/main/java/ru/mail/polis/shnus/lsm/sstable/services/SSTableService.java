package ru.mail.polis.shnus.lsm.sstable.services;

import ru.mail.polis.shnus.lsm.sstable.model.SSTableLocation;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

public class SSTableService {

    private static File data;

    public SSTableService(File data) {
        SSTableService.data = data;
    }

    public SSTableService() {

    }


    private byte[] getBytesByOffset(long fileNumber, long offset, long length) throws IOException {
        RandomAccessFile ras = new RandomAccessFile(Utils.getPath(data, Utils.getDataNameByNumber((int) fileNumber)), "rws");
        byte[] bytes = new byte[(int) length];
        ras.seek(offset);
        ras.read(bytes, 0, (int) length);
        ras.close();
        return bytes;
    }

    private byte[] getFastBytesByOffset(MappedByteBuffer mappedByteBuffer, long offset, long length) throws IOException {
        byte[] bytes = new byte[(int) length];

        mappedByteBuffer.position((int) offset);
        mappedByteBuffer.get(bytes, 0, (int) length);

        return bytes;
    }

    public byte[] getBytesFromSSTable(SSTableLocation valueLocation) throws IOException {
        return getBytesByOffset(valueLocation.getFileNumber(), valueLocation.getOffset(), valueLocation.getLength());
    }

    public byte[] getFastBytesFromSSTable(SSTableLocation valueLocation) throws IOException {
        return getFastBytesByOffset(valueLocation.getMappedByteBuffer(), valueLocation.getOffset(), valueLocation.getLength());
    }


    public long getLengthByNumber(long fileNumber) throws IOException {
        RandomAccessFile ras = new RandomAccessFile(Utils.getPath(data, Utils.getDataNameByNumber((int) fileNumber)), "rws");
        long fileLength = ras.length();
        ras.close();
        return fileLength;
    }

}
