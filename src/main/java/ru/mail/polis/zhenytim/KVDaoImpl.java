package ru.mail.polis.zhenytim;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.*;

public class KVDaoImpl implements KVDao {

    private final Map<String, byte[]> data = new TreeMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        if(!data.containsKey(new String(key))) throw new NoSuchElementException();
        return data.get(new String(key));
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        data.put(new String(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        data.remove(new String(key));
    }

    @Override
    public void close() throws IOException {

    }
}
