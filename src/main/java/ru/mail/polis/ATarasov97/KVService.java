package ru.mail.polis.ATarasov97;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;


import ru.mail.polis.KVDao;

public class KVService implements KVDao {

    private final Map<String, byte[]> data = new HashMap<>();

    private String wrapKey(byte[] key) {
        StringBuilder builder = new StringBuilder();
        for (byte value : key) {
            builder.append(value);
        }
        return builder.toString();
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
