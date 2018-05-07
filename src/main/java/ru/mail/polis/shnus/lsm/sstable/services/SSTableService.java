package ru.mail.polis.shnus.lsm.sstable.services;

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SSTableService implements Closeable {

    private static File data;

    public SSTableService(File data) {
        SSTableService.data = data;
    }

    public SSTableService() {

    }

    public static void unmap(MappedByteBuffer buffer) {
        sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();
    }

    private byte[] getBytesByOffset(long fileNumber, long offset, long length) throws IOException {
        RandomAccessFile ras = new RandomAccessFile(Utils.getPath(data, Utils.getDataNameByNumber((int) fileNumber)), "rws");
        byte[] bytes = new byte[(int) length];
        ras.seek(offset);
        ras.read(bytes, 0, (int) length);
        ras.close();
        return bytes;
    }

    private byte[] getFastBytesByOffset(FileChannel fileChannel, long offset, long length, long fileNumber) throws IOException {
        byte[] bytes = new byte[(int) length];


        FileChannel channel =  FileChannel.open(Paths.get(Utils.getPath(data, Utils.getDataNameByNumber((int) fileNumber))));

        MappedByteBuffer in = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
        in.get(bytes, 0, (int) length);

        unmap(in);

        channel.close();

        return bytes;
    }

    public byte[] getBytesFromSSTable(SSTableLocation valueLocation) throws IOException {
        return getBytesByOffset(valueLocation.getFileNumber(), valueLocation.getOffset(), valueLocation.getLength());
    }

    public byte[] getFastBytesFromSSTable(SSTableLocation valueLocation) throws IOException {
        return getFastBytesByOffset(valueLocation.getFileChannel(),  valueLocation.getOffset(), valueLocation.getLength(), valueLocation.getFileNumber());
    }



    public long getLengthByNumber(long fileNumber) throws IOException {
        RandomAccessFile ras = new RandomAccessFile(Utils.getPath(data, Utils.getDataNameByNumber((int) fileNumber)), "rws");
        long fileLength = ras.length();
        ras.close();
        return fileLength;
    }

    @Override
    public void close() throws IOException {
     //   for(Map.Entry<String, FileChannel> entry: fileChannelMap.entrySet()){
       //     entry.getValue().close();
      //  }
    }
}
