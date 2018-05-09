package ru.mail.polis.poletova_n;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    MVMap<byte[], byte[]> map;
    MVStore s;


    public KVDaoImpl(File file){
        s = new MVStore.Builder().autoCommitBufferSize(128)
                .fileName(file.getAbsolutePath() + File.separator + "file.db")
                .open();
        map = s.openMap("ba2ba");
    }
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] a = map.get(key);
        if (a == null)
        {
            throw new NoSuchElementException();
        }
        return a;
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
        System.out.println(s.isClosed());
    }
}
