package ru.mail.polis.alexeykotelevskiy;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    MVMap<byte[], byte[]> map;
    MVStore s;

    public KVDaoImpl(File data) {
        s = new MVStore.Builder()
                .fileName(data.getAbsolutePath() + File.separator + "data.db")
                .cacheSize(128)
                .open();
        map = s.openMap("ba2ba");
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] val = map.get(key);
        if (val == null)
        {
            throw new NoSuchElementException();
        }
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        s.close();
    }
}