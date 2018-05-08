package ru.mail.polis.shnus.lsm.sstable;

import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.model.KeyAndOffset;
import ru.mail.polis.shnus.lsm.sstable.model.SSTableLocation;
import ru.mail.polis.shnus.lsm.sstable.services.SSTableService;
import ru.mail.polis.shnus.lsm.sstable.services.Utils;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Index implements Closeable {
    static File indexPath;
    static File dataPath;
    private SSTableService ssTableService = new SSTableService();
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;

    //index number which correspond to data number
    private long indexNumber;
    private long timeStamp;
    private List<KeyAndOffset> index;

    Index(long indexNumber) throws IOException {
        this.indexNumber = indexNumber;
        uploadIndexByNumber();
    }

    public Index(List<KeyAndOffset> index, long fileNumber, long timeStamp) throws IOException {
        indexNumber = fileNumber;
        this.timeStamp = timeStamp;
        this.index = index;
        initIO();
    }

    public static void unmap(MappedByteBuffer buffer) {
        sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        if(cleaner != null) {
            cleaner.clean();
        }
    }

    private void uploadIndexByNumber() throws IOException {
        uploadIndexByPath();
    }

    //Extremly unreadable and duplicated code
    //Should be rewrited
    private void uploadIndexByPath() throws IOException {
        //create file channel and mappedByteBuffer for this index
        initIO();

        index = new ArrayList<>();

        String indexFilePath = Utils.getPath(indexPath, Utils.getIndexNameByNumber((int) indexNumber));
        RandomAccessFile ras = new RandomAccessFile(indexFilePath, "rw");
        long fileLength = ras.length();

        byte[] timeStampBytes = new byte[8];
        ras.seek(0);
        ras.read(timeStampBytes, 0, 8);
        timeStamp = Utils.bytesToLong(timeStampBytes);

        byte[] valueOffsetBytes = new byte[8];
        byte[] nextKeyOffsetBytes = new byte[8];
        byte[] realKey;
        long realKeyOffset;
        long realKeyLength;
        long valueOffset;
        long valueLength;
        long nextKeyOffset;
        long indexPosition;

        long currentPosition = 8;

        byte[] isRemovedChecker = new byte[1];

        while(true){
            ras.seek(currentPosition);
            ras.read(isRemovedChecker, 0, 1);
            if(isRemovedChecker[0]==0){
                currentPosition+=1;
                break;
            } else {
                currentPosition+=17;
                if(currentPosition >= fileLength){
                    ras.close();
                    return;
                }
            }
        }

        ras.seek(currentPosition);
        ras.read(nextKeyOffsetBytes, 0, 8);
        nextKeyOffset = Utils.bytesToLong(nextKeyOffsetBytes);
        currentPosition += 8;

        while (currentPosition < fileLength) {
            indexPosition = currentPosition - 8;

            realKeyOffset = nextKeyOffset;

            ras.seek(currentPosition);
            ras.read(valueOffsetBytes, 0, 8);
            currentPosition += 8;

            if (currentPosition >= fileLength) {
                nextKeyOffset = ssTableService.getLengthByNumber(indexNumber);
            } else {
                ras.seek(currentPosition);
                ras.read(isRemovedChecker, 0, 1);
                currentPosition+=1;

                ras.seek(currentPosition);
                ras.read(nextKeyOffsetBytes, 0, 8);
                currentPosition += 8;
                nextKeyOffset = Utils.bytesToLong(nextKeyOffsetBytes);
            }

            valueOffset = Utils.bytesToLong(valueOffsetBytes);
            valueLength = nextKeyOffset - valueOffset;
            realKeyLength = valueOffset - realKeyOffset;

            realKey = ssTableService.getFastBytesFromSSTable(new SSTableLocation(mappedByteBuffer, indexNumber, realKeyOffset, realKeyLength));

            index.add(new KeyAndOffset(new ByteWrapper(realKey), valueOffset, valueLength, indexPosition));

            //End of file
            //Index fully uploaded
            if(currentPosition>=fileLength){
                break;
            }

            //If next key is exist and non deleted, upload it to index
            if(isRemovedChecker[0] == 0){
                continue;
            }

            //If the next key is deleted, check is next next key exist
            currentPosition+=8;
            if(currentPosition >= fileLength) {
                break;
            }

            //Search for the next non deleted value
            while(true){
                ras.seek(currentPosition);
                ras.read(isRemovedChecker, 0, 1);
                if(isRemovedChecker[0]==0){
                    currentPosition+=1;
                    ras.seek(currentPosition);
                    ras.read(nextKeyOffsetBytes, 0, 8);
                    currentPosition += 8;
                    nextKeyOffset = Utils.bytesToLong(nextKeyOffsetBytes);
                    break;
                } else {
                    currentPosition+=17;
                    if(currentPosition >= fileLength){
                        break;
                    }
                }
            }
        }
        ras.close();
    }

    public void markAsRemoved(int position) throws IOException {
        String indexFilePath = Utils.getPath(indexPath, Utils.getIndexNameByNumber((int) indexNumber));
        RandomAccessFile ras = new RandomAccessFile(indexFilePath, "rw");

        ras.seek(index.get(position).getIndexPosition()-1);

        //write non zero value to remove marker
        byte[] removeMarker = new byte[1];
        removeMarker[0] = 1;

        ras.write(removeMarker);
        ras.close();

    }

    public void removeFromMemory(ByteWrapper keyWrapper) {
        for(int i = 0; i < index.size(); i++){
            if(index.get(i).getKey().equals(keyWrapper)){
                index.remove(i);
                break;
            }
        }

    }

    private void initIO() throws IOException {
        fileChannel = new FileInputStream(Utils.getPath(dataPath, Utils.getDataNameByNumber((int) indexNumber))).getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
    }

    SSTableLocation findAndGetKeyLocation(ByteWrapper keyWrapper) {
        KeyAndOffset key = new KeyAndOffset(keyWrapper, -1, -1, -1);
        int pos = Collections.binarySearch(index, key);
        if (pos < 0) {
            return null;
        }
        KeyAndOffset keyAndOffset = index.get(pos);
        return new SSTableLocation(mappedByteBuffer, indexNumber, keyAndOffset.getOffset(), keyAndOffset.getLength());
    }

    int findAndGetRemoveMarkerLocation(ByteWrapper keyWrapper) {
        KeyAndOffset key = new KeyAndOffset(keyWrapper, -1, -1, -1);
        int pos = Collections.binarySearch(index, key);
        if (pos < 0) {
            return -1;
        }
        return pos;
    }

    long getTimeStamp() {
        return timeStamp;
    }

    public boolean isEmpty(){
        return index.isEmpty();
    }

    @Override
    public void close() throws IOException {
        unmap(mappedByteBuffer);
        fileChannel.close();
    }

}
