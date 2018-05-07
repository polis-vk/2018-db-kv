package ru.mail.polis.shnus.lsm;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.DiskMaster;

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


    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        int insertSize = key.length + value.length;

        if (size + insertSize > 50_000_000) {
            flush();
            size = 0;
            table = new TreeMap<>(); //and hope to avoid outOfMemory by next value
        }

        ByteWrapper keyWrapper = new ByteWrapper(key);

        if (table.containsKey(keyWrapper)) {
            size = size - key.length - table.put(keyWrapper, value).length;
        } else {
            table.put(keyWrapper, value);
        }
        size += insertSize;
    }

    public void test(@NotNull byte[] key, @NotNull byte[] value) {

    }


    @Override
    public void remove(@NotNull byte[] key) throws IOException {

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
