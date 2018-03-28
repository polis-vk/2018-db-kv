package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
        }

    @NotNull
    public static KVDao create() throws IOException {
        return new KVDaoImpl();
    }

    private static class KVDaoImpl implements KVDao{
        private Map<byte[], byte[]> KVStorage;

        public KVDaoImpl() {
            this.KVStorage = new HashMap<>();
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
            final byte[] storedValue = this.KVStorage.get(key);
            if (storedValue == null) throw new NoSuchElementException();
            return storedValue;
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            final byte[] storedValue = this.KVStorage.put(key, value);
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException {
            final byte[] value = this.KVStorage.remove(key);
            if (value == null) throw new NoSuchElementException();
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
