package ru.mail.polis.poletova_n;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    DB db;
    ConcurrentMap<String,byte[]> map;


    public KVDaoImpl(File file){
        db = DBMaker.fileDB("file.db")
                .fileMmapEnable()
                .make();
        map = db.hashMap("map",Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen();

    }
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteArray k = new ByteArray(key);
        byte [] value = map.get(k.toString());
        if(value==null){
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(new ByteArray(key).toString(),value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove(new ByteArray(key).toString());
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
