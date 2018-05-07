package ru.mail.polis.kirillyarulin;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by Kirill Yarulin on 06.05.18
 */
public class DiskKVDao2 implements KVDao {

    private final DB db;
    private final HTreeMap<byte[], byte[]> map;

    public DiskKVDao2(File directory) {
        this.db = DBMaker
                .fileDB(new File(directory,"file.db"))
                .fileMmapEnableIfSupported()
                .make();
        this.map = db
                .hashMap("storage", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
                .createOrOpen();

    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] result = map.get(key);
        if (result != null) {
            return result;
        } else {
            throw new NoSuchElementException();
        }
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
        db.close();
    }
}
