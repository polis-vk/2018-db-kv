package ru.mail.polis.gskoba;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVServiceFactory implements KVDao {


    private final Map<ByteBuffer, byte[]> storage;

    public KVServiceFactory() {
        this.storage = new HashMap<>();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final byte[] out = this.storage.get(ByteBuffer.wrap(key));
        if (out == null) throw new NoSuchElementException();
        return out;
    }


    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        this.storage.put(ByteBuffer.wrap(key),value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException{
        this.storage.remove(ByteBuffer.wrap(key));
    }

    @Override
    public void close() throws IOException{

    }
}
