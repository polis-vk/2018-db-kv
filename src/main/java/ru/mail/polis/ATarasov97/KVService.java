package ru.mail.polis.ATarasov97;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVService implements KVDao {

    private final Map<String, byte[]> data = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        if (!data.containsKey(Arrays.toString(key))) {
            throw new NoSuchElementException("No element with key:" + Arrays.toString(key));
        }
        return data.get(Arrays.toString(key));
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        data.put(Arrays.toString(key), value);
        String a = "aaa";
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        data.remove(Arrays.toString(key));
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }
}
