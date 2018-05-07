package ru.mail.polis.shnus.lsm.sstable;

import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.model.KeyAndOffset;
import ru.mail.polis.shnus.lsm.sstable.model.SSTableLocation;
import ru.mail.polis.shnus.lsm.sstable.services.SSTableService;
import ru.mail.polis.shnus.lsm.sstable.services.Utils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Index implements Closeable {
    static File indexPath;
    static File dataPath;
    private SSTableService ssTableService = new SSTableService();
    private FileChannel fileChannel;

    //index number which correspond to data number
    private long indexNumber;
    private long timeStamp;
    private List<KeyAndOffset> index;

    Index(long indexNumber) throws IOException {
        this.indexNumber = indexNumber;
        uploadIndexByNumber();
    }

    Index(String path) throws IOException {
        indexNumber = Utils.getNumberFromIndexPath(path);
        uploadIndexByPath();
    }

    public Index(List<KeyAndOffset> index, long fileNumber, long timeStamp) throws FileNotFoundException {
        indexNumber = fileNumber;
        this.timeStamp = timeStamp;
        this.index = index;
        fileChannel = new FileInputStream(Utils.getPath(dataPath, Utils.getDataNameByNumber((int) indexNumber))).getChannel();

    }

    private void uploadIndexByNumber() throws IOException {
        uploadIndexByPath();
    }

    private void uploadIndexByPath() throws IOException {
        //create file channel for this index

        fileChannel = new FileInputStream(Utils.getPath(dataPath, Utils.getDataNameByNumber((int) indexNumber))).getChannel();

        index = new ArrayList<>();

        String indexFilePath = Utils.getPath(indexPath, Utils.getIndexNameByNumber((int) indexNumber));
        RandomAccessFile ras = new RandomAccessFile(indexFilePath, "rwd");

        byte[] timeStampBytes = new byte[8];
        ras.seek(0);
        ras.read(timeStampBytes, 0, 8);
        timeStamp = Utils.bytesToLong(timeStampBytes);

        byte[] offsetBytes = new byte[8];
        byte[] nextKeyOffsetBytes = new byte[8];
        byte[] realKey;
        long realKeyOffset;
        long realKeyLength;
        long valueOffset;
        long valueLength;
        long nextKeyOffset;

        long currentPosition = 8;

        ras.seek(currentPosition);
        ras.read(nextKeyOffsetBytes, 0, 8);
        nextKeyOffset = Utils.bytesToLong(nextKeyOffsetBytes);
        currentPosition += 8;

        while (currentPosition < ras.length()) {

            realKeyOffset = nextKeyOffset;

            ras.seek(currentPosition);
            ras.read(offsetBytes, 0, 8);
            currentPosition += 8;

            if (currentPosition >= ras.length()) {
                nextKeyOffset = ssTableService.getLengthByNumber(indexNumber);
            } else {
                ras.seek(currentPosition);
                ras.read(nextKeyOffsetBytes, 0, 8);
                currentPosition += 8;
                nextKeyOffset = Utils.bytesToLong(nextKeyOffsetBytes);
            }

            valueOffset = Utils.bytesToLong(offsetBytes);
            valueLength = nextKeyOffset - valueOffset;
            realKeyLength = valueOffset - realKeyOffset;

            realKey = ssTableService.getFastBytesFromSSTable(new SSTableLocation(fileChannel, indexNumber, realKeyOffset, realKeyLength));

            index.add(new KeyAndOffset(new ByteWrapper(realKey), valueOffset, valueLength));
        }
        ras.close();
    }

    SSTableLocation findAndGetKeyLocation(ByteWrapper keyWrapper) {
        KeyAndOffset key = new KeyAndOffset(keyWrapper, -1, -1);
        int pos = Collections.binarySearch(index, key);
        if (pos < 0) {
            return null;
        }
        KeyAndOffset keyAndOffset = index.get(pos);
        return new SSTableLocation(fileChannel, indexNumber, keyAndOffset.getOffset(), keyAndOffset.getLength());
    }

    long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

}
