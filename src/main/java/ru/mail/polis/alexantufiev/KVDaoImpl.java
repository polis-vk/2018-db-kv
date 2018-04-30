package ru.mail.polis.alexantufiev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

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

    private Map<byte[], byte[]> dictionary;

    public KVDaoImpl() {
        dictionary = new HashMap<>();
    }

    public KVDaoImpl(HashMap<byte[], byte[]> dictionary) {
        this.dictionary = dictionary;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        for (Map.Entry<byte[], byte[]> entries : dictionary.entrySet()) {
            if (Arrays.equals(entries.getKey(), key)) {
                return entries.getValue();
            }
        }
        throw new NoSuchElementException("Element with key = ?" + Arrays.toString(key) + "is not found");
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        dictionary.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        dictionary.remove(key);
    }

    @Override
    public void close() {
        dictionary.clear();
    }
}
