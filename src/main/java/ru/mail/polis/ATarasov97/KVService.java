package ru.mail.polis.ATarasov97;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import ru.mail.polis.KVDao;

public class KVService implements KVDao {

    private final DB db;
    private final HTreeMap<byte[], byte[]> store;

    public KVService(@NotNull final File data) {
        File dbData = new File(data, "db");
        this.db = DBMaker
                .fileDB(dbData)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .make();
        this.store = db.hashMap(dbData.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] bytes = store.get(key);
        if (bytes == null) {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        store.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        store.remove(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
