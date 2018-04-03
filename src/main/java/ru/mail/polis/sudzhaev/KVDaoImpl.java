package ru.mail.polis.sudzhaev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<String, byte[]> map = new HashMap<>();

    private String serializeKey(byte[] key) {
        StringBuilder builder = new StringBuilder();
        for (byte b : key) {
            builder.append(b);
        }
        return builder.toString();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        String strKey = serializeKey(key);
        if (!map.containsKey(strKey)) throw new NoSuchElementException();
        return map.get(strKey);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        String strKey = serializeKey(key);
        map.put(strKey, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        String strKey = serializeKey(key);
        map.remove(strKey);
    }

    @Override
    public void close() {
        map.clear();
    }
}
