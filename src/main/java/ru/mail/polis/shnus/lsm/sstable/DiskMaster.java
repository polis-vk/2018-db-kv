package ru.mail.polis.shnus.lsm.sstable;

import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.services.SSTableLocation;
import ru.mail.polis.shnus.lsm.sstable.services.SSTableService;
import ru.mail.polis.shnus.lsm.sstable.services.Utils;

import java.io.*;
import java.sql.Timestamp;
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
        String indexPath = data.getPath() + File.separator + "index";
        if (new File(indexPath).mkdir()) {
            return new File(indexPath);
        } else {
            //dont know exactly is data exist but hope
            return new File(indexPath);
        }
    }

    public void flush(Map<ByteWrapper, byte[]> table) throws IOException {
        String[] newTableAndIndexPath = getTableAndIndexPath(getNextTableAndIndexName());
        Timestamp timestamp;
        try (
                OutputStream newTable = new FileOutputStream(newTableAndIndexPath[0]);
                OutputStream newIndex = new FileOutputStream(newTableAndIndexPath[1])
        ) {
            byte[] key;
            byte[] value;
            long offset = 0;
            timestamp = new Timestamp(System.currentTimeMillis());
            //each index file has timestamp on the top
            newIndex.write(Utils.longToBytes(timestamp.getTime()));
            for (Map.Entry<ByteWrapper, byte[]> entry : table.entrySet()) {
                key = entry.getKey().getBytes();
                value = entry.getValue();

                //writing to sstable
                newTable.write(key);
                newTable.write(value);

                //writing to index file
                //offset of key instead of original key,
                // because i dont know how to resolve different length, line break and separator problems
                newIndex.write(Utils.longToBytes(offset));
                offset += key.length;

                newIndex.write(Utils.longToBytes(offset));
                offset += value.length;
            }
        }
        //upload index to the memory, non efficiently but anyway
      //  indexTables.addIndexByMap(table, Utils.getNumberFromIndexPath(newTableAndIndexPath[1]), timestamp.getTime());
        indexTables.upload(newTableAndIndexPath[1]);
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
        tableAndIndexName[0] = new StringBuilder("data")
                .append(Utils.SEPARATOR).append(nextFileNumber)
                .toString();
        tableAndIndexName[1] = new StringBuilder("data")
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
            if (fileName.compareTo("index") != 0) {
                int currentFileNumber = Integer.valueOf(fileName.split("_")[1]);
                n = Math.max(n, currentFileNumber);
            }
        }

        return n + 1;
    }

    @Override
    public void close() throws IOException {
        indexTables.close();
    }
}
