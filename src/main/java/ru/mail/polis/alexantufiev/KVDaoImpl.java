package ru.mail.polis.alexantufiev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Implementation of KVDao.
 *
 * @author Aleksey Antufev
 * @version 1.0.0
 * @since 1.0.0 30.04.2018
 */
public class KVDaoImpl implements KVDao {

    private Map<ByteBuffer, byte[]> dictionary;

    public KVDaoImpl() {
        dictionary = new HashMap<>();
    }

    public KVDaoImpl(HashMap<ByteBuffer, byte[]> dictionary) {
        this.dictionary = dictionary;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        byte[] temp = dictionary.get(ByteBuffer.wrap(key));
        if (temp == null) {
            throw new NoSuchElementException("Element with key = ?" + Arrays.toString(key) + "is not found");
        }
        return temp;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        dictionary.put(ByteBuffer.wrap(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        dictionary.remove(ByteBuffer.wrap(key));
    }

    @Override
    public void close() {
        dictionary.clear();
    }
}
