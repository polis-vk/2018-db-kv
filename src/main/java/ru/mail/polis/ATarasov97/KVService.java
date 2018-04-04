package ru.mail.polis.ATarasov97;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;


import ru.mail.polis.KVDao;

public class KVService implements KVDao {

    private final Map<ByteBuffer, byte[]> data = new HashMap<>();

    private ByteBuffer wrapKey(byte[] key) {
        return ByteBuffer.wrap(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        if (!data.containsKey(wrapKey(key))) {
            throw new NoSuchElementException("No element with key:" + Arrays.toString(key));
        }
        return data.get(wrapKey(key));
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        data.put(wrapKey(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        data.remove(wrapKey(key));
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }
}
