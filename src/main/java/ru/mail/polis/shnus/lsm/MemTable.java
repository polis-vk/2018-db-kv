package ru.mail.polis.shnus.lsm;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.DiskMaster;
import ru.mail.polis.shnus.lsm.sstable.services.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class MemTable implements KVDao {
    private final DiskMaster diskMaster;
    private Map<ByteWrapper, byte[]> table;
    private int size;


    public MemTable(File data) throws IOException {
        diskMaster = new DiskMaster(data);
        table = new TreeMap<>();
        size = 0;
    }


    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value;
        value = table.get(new ByteWrapper(key));

        if (value != null) {
            return value;
        }

        value = diskMaster.getValueByKey(key);

        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    //Should be optimized.
    //After flushing it is not good that in memory data(tree map) is empty
    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        int insertSize = key.length + value.length;

        if (size + insertSize > Utils.SSTABLE_FILE_SIZE) {
            flush();
            size = 0;
            table = new TreeMap<>();
        }

        ByteWrapper keyWrapper = new ByteWrapper(key);

        if (table.containsKey(keyWrapper)) {
            size = size - key.length - table.put(keyWrapper, value).length;
        } else {
            //Hope to avoid outOfMemory by next value if it is extremely large (over 90Mb)
            table.put(keyWrapper, value);
        }
        size += insertSize;
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteWrapper keyWrapper = new ByteWrapper(key);
        table.remove(keyWrapper);

        diskMaster.findAndRemove(keyWrapper);
    }

    private void flush() throws IOException {
        diskMaster.flush(table);
    }


    @Override
    public void close() throws IOException {
        if (table.size() > 0) {
            flush();
        }
        diskMaster.close();
    }
}
