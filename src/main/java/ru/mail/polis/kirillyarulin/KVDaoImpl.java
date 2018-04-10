package ru.mail.polis.kirillyarulin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {
    private final Map<ByteBuffer, byte[]> storage = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value = storage.get(ByteBuffer.wrap(key));

        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        storage.put(ByteBuffer.wrap(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        storage.remove(ByteBuffer.wrap(key));
    }

    @Override
    public void close() throws IOException {

    }

}
