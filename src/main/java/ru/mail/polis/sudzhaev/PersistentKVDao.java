package ru.mail.polis.sudzhaev;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;

public class PersistentKVDao implements KVDao {

    private final DB db;
    private final HTreeMap<byte[], byte[]> storage;

    public PersistentKVDao(File directory) {
        File data = new File(directory, "db");
        this.db = DBMaker
                .fileDB(data)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        byte[] bytes = storage.get(key);
        if (bytes == null) {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        storage.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        storage.remove(key);
    }

    @Override
    public void close() {
        db.close();
    }
}
