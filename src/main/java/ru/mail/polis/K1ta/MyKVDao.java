package ru.mail.polis.K1ta;

import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import ru.mail.polis.KVDao;

import java.io.*;
import java.util.NoSuchElementException;

public class MyKVDao implements KVDao {
    DB db;
    HTreeMap<byte[], byte[]> map;

    public MyKVDao(File data) {
        db = DBMaker
                .fileDB(data.getAbsolutePath() + "//db")
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        map = db
                .hashMap("data")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] val = map.get(key);
        if (val == null) throw new NoSuchElementException();
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        map.remove(key);
    }

    @Override
    public void close() {
        db.close();
    }
}
