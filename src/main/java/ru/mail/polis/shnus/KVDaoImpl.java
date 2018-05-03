package ru.mail.polis.shnus;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteWrapper, byte[]> storage;

    public KVDaoImpl() {
        storage = new HashMap<>();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value = storage.get(new ByteWrapper(key));
        if (value == null) {
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        storage.put(new ByteWrapper(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        storage.remove(new ByteWrapper(key));
    }

    @Override
    public void close() throws IOException {

    }
}
