package ru.mail.polis.zhenytim;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteBuffer, byte[]> data = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final byte[] res = data.get(ByteBuffer.wrap(key));
        if(res == null) throw new NoSuchElementException();
        return res;
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
