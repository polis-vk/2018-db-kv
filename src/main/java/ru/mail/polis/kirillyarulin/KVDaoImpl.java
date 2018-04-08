package ru.mail.polis.kirillyarulin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {
    private Map<ByteArrayWrapper, ByteArrayWrapper> storage = new HashMap<>();

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        if (storage.isEmpty() || !storage.containsKey(new ByteArrayWrapper(key))) {
            throw new NoSuchElementException();
        }

        return storage.get(new ByteArrayWrapper(key)).getArray();
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        storage.put(new ByteArrayWrapper(key), new ByteArrayWrapper(value));
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        storage.remove(new ByteArrayWrapper(key));
    }

    @Override
    public void close() throws IOException {

    }


    private class ByteArrayWrapper {
        private byte[] array;

        public ByteArrayWrapper(byte[] array) {
            this.array = array;
        }

        public byte[] getArray() {
            return array;
        }

        public void setArray(byte[] arr) {
            this.array = array;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }
}
