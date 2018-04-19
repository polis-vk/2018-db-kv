/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Custom {@link KVDao} factory
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public final class KVDaoFactory {
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    private KVDaoFactory() {
        // Not instantiatable
    }

    /**
     * Construct a {@link KVDao} instance.
     *
     * @param data local disk folder to persist the data to
     * @return a storage instance
     */
    @NotNull
    public static KVDao create(@NotNull final File data) throws IOException {
//        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
//            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
//        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }

        return new KVDaoImpl(data);
    }

    private class StoredItem {
        //implement me
    }

    private static class KVDaoImpl implements KVDao {
        final private String dirWithSep;

        private final Map<ByteBuffer, StoredItem> storage;

        public KVDaoImpl(File dir) {
            dirWithSep = dir + File.separator;
            storage = new HashMap<>();
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
            final StoredItem storedValue = this.storage.get(ByteBuffer.wrap(key));
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
