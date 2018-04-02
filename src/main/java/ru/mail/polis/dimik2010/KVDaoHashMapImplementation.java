package ru.mail.polis.dimik2010;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoHashMapImplementation implements KVDao {
    private final Map<ByteBuffer, byte[]> data = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        if (!data.containsKey(ByteBuffer.wrap(key))) {
            throw new NoSuchElementException("No such element with key = " + Arrays.toString(key));
        }
        return data.get(ByteBuffer.wrap(key));
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
