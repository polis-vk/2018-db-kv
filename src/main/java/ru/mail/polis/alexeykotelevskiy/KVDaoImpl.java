package ru.mail.polis.alexeykotelevskiy;

import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {
    private HashMap<String, byte[]> hashMap = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        String strKey= new String(key);
        if (!hashMap.containsKey(strKey)) throw new NoSuchElementException();
        return hashMap.get(strKey);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {

        hashMap.put(new String(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        hashMap.remove(new String(key));;
    }

    @Override
    public void close() throws IOException {

    }
}
