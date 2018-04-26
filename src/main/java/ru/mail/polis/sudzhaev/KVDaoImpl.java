package ru.mail.polis.sudzhaev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteWrapper, byte[]> map = new HashMap<>();

    private ByteWrapper serializeKey(byte[] key) {
        return new ByteWrapper(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        byte[] bytes = map.get(serializeKey(key));
        if (bytes == null) throw new NoSuchElementException();
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        map.put(serializeKey(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        map.remove(serializeKey(key));
    }

    @Override
    public void close() {
    }
}
