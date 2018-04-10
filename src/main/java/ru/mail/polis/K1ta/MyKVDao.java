package ru.mail.polis.K1ta;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.NoSuchElementException;

public class MyKVDao implements KVDao {
    HashMap<ByteBuffer, byte[]> map = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        byte[] val = map.get(_key);
        if (val == null) {
            throw new NoSuchElementException();
        }
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        map.put(_key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        map.remove(_key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
