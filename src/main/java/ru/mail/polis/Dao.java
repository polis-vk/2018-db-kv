package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;


public class Dao implements KVDao {

    private final Map<ByteWrapper, byte[]> map = new HashMap<>();

    private ByteWrapper serializeKey(byte[] key) {
        return new ByteWrapper(key);
    }

    @NotNull
    @Override
    public byte[] get(byte[] key) throws NoSuchElementException, IOException {
        byte[] bytes = map.get(serializeKey(key));
        if (bytes == null) throw new NoSuchElementException();
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(serializeKey(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove(serializeKey(key));
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }

}
