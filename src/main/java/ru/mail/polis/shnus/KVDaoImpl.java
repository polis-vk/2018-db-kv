package ru.mail.polis.shnus;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.shnus.lsm.MemTable;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final MemTable memTable;
    private final File path;

    public KVDaoImpl(File path) throws IOException {
        this.path = path;
        memTable = new MemTable(path);
    }


    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value = memTable.get(key);

        if (value == null) {
            throw new NoSuchElementException();
        } else {
            return value;
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        memTable.upsert(key, value);
    }

    public void test() {
        memTable.test(new byte[]{0}, new byte[]{1});
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {

    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
