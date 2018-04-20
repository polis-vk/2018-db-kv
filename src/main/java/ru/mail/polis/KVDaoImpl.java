package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteBuffer, byte[]> data;

    public KVDaoImpl() {
        this.data = new HashMap<>();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key);
        byte[] result = data.get(wrappedKey);
        if (result == null) throw new NoSuchElementException();
        return data.get(wrappedKey);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        data.put(ByteBuffer.wrap(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        data.remove(ByteBuffer.wrap(key));
    }

    @Override
    public void close() throws IOException {
    }
}
