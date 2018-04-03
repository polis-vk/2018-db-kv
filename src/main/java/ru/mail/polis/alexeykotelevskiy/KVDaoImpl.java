package ru.mail.polis.alexeykotelevskiy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {
    private HashMap<ByteBuffer, byte[]> hashMap = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteBuffer bKey = ByteBuffer.wrap(key);
        if (!hashMap.containsKey(bKey)) throw new NoSuchElementException();
        return hashMap.get(bKey);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {

        hashMap.put(ByteBuffer.wrap(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        hashMap.remove(ByteBuffer.wrap(key));
    }

    @Override
    public void close() throws IOException {

    }
}
