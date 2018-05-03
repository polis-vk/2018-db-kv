package ru.mail.polis.gskoba;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVServiceFactory implements KVDao {


    private final DB db;
    private final File data;
    private final HTreeMap<byte[], byte[]> storage;

    public KVServiceFactory(File directory) {
        this.data = new File(directory,"db");
        this.db = DBMaker.fileDB(data).make();
        this.storage = db.hashMap(data.getName()).keySerializer(Serializer.BYTE_ARRAY).valueSerializer(Serializer.BYTE_ARRAY).createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final byte[] out = this.storage.get(key);
        if (out == null) throw new NoSuchElementException();
        return out;
    }


    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        this.storage.put(key,value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException{
        this.storage.remove(key);
    }

    @Override
    public void close() throws IOException{
        db.close();
    }
}
