package ru.mail.polis.vana06;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteArrayWrapper, byte[]> map = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value = map.get(new ByteArrayWrapper(key));
        if(value == null){
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        map.remove(new ByteArrayWrapper(key));
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}
