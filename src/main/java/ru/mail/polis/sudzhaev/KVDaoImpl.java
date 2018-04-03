package ru.mail.polis.sudzhaev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<BigInteger, byte[]> map = new HashMap<>();

    private BigInteger serializeKey(byte[] key) {
        return new BigInteger(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        BigInteger strKey = serializeKey(key);
        if (!map.containsKey(strKey)) throw new NoSuchElementException();
        return map.get(strKey);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        BigInteger strKey = serializeKey(key);
        map.put(strKey, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        BigInteger strKey = serializeKey(key);
        map.remove(strKey);
    }

    @Override
    public void close() {
        map.clear();
    }
}
