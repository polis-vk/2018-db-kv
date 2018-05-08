package ru.mail.polis.shnus.lsm.sstable;

import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.model.KeyAndOffset;
import ru.mail.polis.shnus.lsm.sstable.model.SSTableLocation;
import ru.mail.polis.shnus.lsm.sstable.services.SSTableService;
import ru.mail.polis.shnus.lsm.sstable.services.Utils;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DiskMaster implements Closeable {
    private final SSTableService ssTableService;
    private final IndexTables indexTables;

    private File data;
    private File index;

    public DiskMaster(File data) throws IOException {
        this.data = data;
        index = getIndexDirectory();
        ssTableService = new SSTableService(data);
        indexTables = new IndexTables(data, index);
    }

    private File getIndexDirectory() {
        String indexPath = data.getPath() + File.separator + Utils.INDEX_FOLDER;
        if (new File(indexPath).mkdir()) {
            return new File(indexPath);
        } else {
            //Don't know exactly is data exist but hope
            return new File(indexPath);
        }
    }

    public void flush(Map<ByteWrapper, byte[]> table) throws IOException {
        String[] newTableAndIndexPath = getTableAndIndexPath(getNextTableAndIndexName());
        Timestamp timestamp;
        List<KeyAndOffset> index = new ArrayList<>();
        long indexPosition = Utils.LONG_TO_BYTE_LENGTH + Utils.BOOLEAN_TO_BYTE_LENGTH;
        try (
                OutputStream newTable = new FileOutputStream(newTableAndIndexPath[0]);
                BufferedOutputStream bufNewTable = new BufferedOutputStream(newTable, Utils.BUFFER_SIZE);
                OutputStream newIndex = new FileOutputStream(newTableAndIndexPath[1]);
                BufferedOutputStream bufNewIndex = new BufferedOutputStream(newIndex, Utils.BUFFER_SIZE)
        ) {
            byte[] key;
            byte[] value;
            long offset = 0;
            timestamp = new Timestamp(System.currentTimeMillis());
            byte[] timeStampByte = Utils.longToBytes(timestamp.getTime());

            //each index file has timestamp on the top
            bufNewIndex.write(timeStampByte, 0, timeStampByte.length);
            for (Map.Entry<ByteWrapper, byte[]> entry : table.entrySet()) {
                key = entry.getKey().getBytes();
                value = entry.getValue();

                //writing to sstable
                bufNewTable.write(key);
                bufNewTable.write(value);

                //"Removed marker" in index file
                //If equals to zero, this key should be loaded to the memory index
                bufNewIndex.write(new byte[]{0}, 0, Utils.BOOLEAN_TO_BYTE_LENGTH);

                //Writing to index file and index in memory
                //Offset of key instead of original key,
                //because i dont know how to resolve different length, line break and separator problems
                //to read original key from index file
                bufNewIndex.write(Utils.longToBytes(offset));
                offset += key.length;
                bufNewIndex.write(Utils.longToBytes(offset));

                //in memory index
                indexPosition += 2 * Utils.LONG_TO_BYTE_LENGTH + Utils.BOOLEAN_TO_BYTE_LENGTH;
                index.add(new KeyAndOffset(entry.getKey(), offset, value.length, indexPosition));
                offset += value.length;
            }
            bufNewTable.flush();
            bufNewIndex.flush();
        }

        indexTables.addIndexByList(index, Utils.getNumberFromIndexPath(newTableAndIndexPath[1]), timestamp.getTime());
    }

    public byte[] getValueByKey(byte[] key) throws IOException {
        SSTableLocation valueLocation = indexTables.getValueLocationByKey(key);
        if (valueLocation == null) {
            return null;
        } else {
            return ssTableService.getFastBytesFromSSTable(valueLocation);
        }
    }

    private String[] getTableAndIndexPath(String[] tableAndIndexName) {
        String[] tableAndIndexPath = new String[2];
        tableAndIndexPath[0] = data + File.separator + tableAndIndexName[0];
        tableAndIndexPath[1] = index + File.separator + tableAndIndexName[1];
        return tableAndIndexPath;
    }

    private String[] getNextTableAndIndexName() {
        String[] tableAndIndexName = new String[2];
        int nextFileNumber = getNextFileNumber();
        tableAndIndexName[0] = new StringBuilder(Utils.DATA_FOLDER)
                .append(Utils.SEPARATOR).append(nextFileNumber)
                .toString();
        tableAndIndexName[1] = new StringBuilder(Utils.DATA_FOLDER)
                .append(Utils.SEPARATOR).append(nextFileNumber)
                .append(Utils.SEPARATOR).append(Utils.INDEX_MARKER)
                .toString();
        return tableAndIndexName;
    }

    private int getNextFileNumber() {
        File[] files = data.listFiles();
        int n = 0;

        //TODO by stream api
        for (File file : files) { //no need to check that file is file and not directory
            String fileName = file.getName();
            //TODO need regex
            if (fileName.compareTo(Utils.INDEX_FOLDER) != 0) {
                int currentFileNumber = Integer.valueOf(fileName.split(Utils.SEPARATOR)[1]);
                n = Math.max(n, currentFileNumber);
            }
        }
        return n + 1;
    }

    public void findAndRemove(ByteWrapper key) throws IOException {
        indexTables.removeValueByKey(key);
    }

    @Override
    public void close() throws IOException {
        indexTables.close();
    }

}
