package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
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
        private final Map<ByteBuffer, byte[]> storage;

        public KVDaoImpl() {
            this.storage = new HashMap<>();
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
            final byte[] storedValue = this.storage.get(ByteBuffer.wrap(key));
            if (storedValue == null) throw new NoSuchElementException();
            return storedValue;
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            this.storage.put(ByteBuffer.wrap(key), value);
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException {
//            final byte[] value = this.storage.remove(ByteBuffer.wrap(key));
//            if (value == null) throw new NoSuchElementException();
            this.storage.remove(ByteBuffer.wrap(key));
        }

        @Override
        public void close() throws IOException {

        }
    }
}
