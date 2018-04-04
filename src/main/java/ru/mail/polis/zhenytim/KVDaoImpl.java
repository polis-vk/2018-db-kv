package ru.mail.polis.zhenytim;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<String, byte[]> data = new HashMap<>();

    public static String bytesToString(byte[] bytes) {
        char[] buffer = new char[bytes.length >> 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            char c = (char) (((bytes[bpos] & 0x00FF) << 8) + (bytes[bpos + 1] & 0x00FF));
            buffer[i] = c;
        }
        return new String(buffer);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final byte[] res = data.get(bytesToString(key));
        if(res == null) throw new NoSuchElementException();
        return res;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        data.put(bytesToString(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        data.remove(bytesToString(key));
    }

    @Override
    public void close() throws IOException {

    }
}
