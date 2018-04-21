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
import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import javax.xml.bind.DatatypeConverter;

import com.sun.javafx.util.Utils;

import jdk.internal.util.xml.impl.Input;
import sun.misc.BASE64Decoder;

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

        return new KVDaoImpl(data, 500);
    }

    private static class StoredValue {
        private byte[] bytes;
        final private File container;

        public StoredValue(byte[] bytes, File container) {
            this.bytes = bytes;
            this.container = container;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(byte[] bytes) {
            this.bytes = bytes;
        }

        public File getContainer() {
            return container;
        }

        //implement me
    }

    private static class KVDaoImpl implements KVDao {
        final private String KV_SPLITTER = "-";
        final private String ENDLINE = "\n";
        final private int KBYTE = 1024;

        final private int TRASH_HOLD;
        final private String STORAGE_DIR;
        private final Map<ByteBuffer, StoredValue> storage;
        private final Queue<File> filesQueue = new LinkedList<>();

        /*
        * Chunk size in kbytes
        */
        public KVDaoImpl(final File dir, final int chunkSize) {
            this.STORAGE_DIR = dir + File.separator;
            this.TRASH_HOLD = chunkSize * KBYTE;
            this.storage = new HashMap<>();
            try {
                java.nio.file.Files.walkFileTree(
                        dir.toPath(),
                        new SimpleFileVisitor<Path>() {
                            private void fetchData(@NotNull final Path file) throws IOException {
                                if (!file.toFile().canRead()) return;
                                InputStream inputStream = new FileInputStream(file.toFile());
                                //reading stored chunk through container
                                String chunk = "";
                                byte[] buffer = new byte[TRASH_HOLD / 4];
                                int len;
                                while ((len = inputStream.read(buffer)) != -1) {
                                    chunk += new String(buffer);
//                                    for (int i = 0; i < len; i++)
//                                        chunk += (char)buffer[i];
                                }
                                inputStream.close();
                                String[] items = chunk.split(ENDLINE);
                                for (String item :
                                        items) {
                                    String[] parts = item.split(KV_SPLITTER);
                                    if (parts.length == 2) {
                                        storage.put(ByteBuffer.wrap(Base64.getDecoder().decode(parts[0].getBytes())),
                                                new StoredValue(Base64.getDecoder().decode(parts[1].getBytes()),
                                                        file.toFile()));
                                    }
                                }
                                if (chunk.length() < TRASH_HOLD) filesQueue.add(file.toFile());
                            }

                            @NotNull
                            @Override
                            public FileVisitResult visitFile(@NotNull final Path file, @NotNull final BasicFileAttributes attrs)
                                    throws IOException {
                                System.out.println(file.getFileName());
                                fetchData(file);
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            };
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
            final StoredValue storedValue = this.storage.get(ByteBuffer.wrap(key));
            if (storedValue == null) throw new NoSuchElementException();
            return storedValue.getBytes();
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            StoredValue item = this.storage.get(ByteBuffer.wrap(key));
            if (item == null) {
                //creating new item and putting into container
                try {
                    this.storage.put(ByteBuffer.wrap(key), new StoredValue(value, create(key, value)));
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            } else {
                //update value in storage and container
                try {
                    update(item.getContainer(), key, value);
                    item.setBytes(value);
                    this.storage.put(ByteBuffer.wrap(key), item);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException {
            final StoredValue item = this.storage.remove(ByteBuffer.wrap(key));
            if (item != null) {
                delete(item.getContainer(), key);
            } else throw new NoSuchElementException();
        }

        @Override
        public void close() throws IOException {

        }

        private File create(byte[] key, byte[] value) throws IOException {
            if (filesQueue.size() == 0) {
                File container = new File(STORAGE_DIR + Long.toString(System.currentTimeMillis()));
                container.createNewFile();
                OutputStream outputStream = new FileOutputStream(container, false);
                outputStream.write((Base64.getEncoder().encodeToString(key) + KV_SPLITTER + Base64.getEncoder().encodeToString(value) + ENDLINE).getBytes());
                outputStream.flush(); outputStream.close();
                if (container.length()  < TRASH_HOLD) this.filesQueue.add(container);
                return container;
            } else {
                File container = this.filesQueue.remove();
                OutputStream outputStream = new FileOutputStream(container, true);
                outputStream.write((Base64.getEncoder().encodeToString(key) + KV_SPLITTER + Base64.getEncoder().encodeToString(value) + ENDLINE).getBytes());
                outputStream.flush(); outputStream.close();
                if (container.length() < TRASH_HOLD) this.filesQueue.add(container);
                return container;
            }

        }

        private void update(File container, byte[] key, byte[] value) throws IOException {
            InputStream inputStream = new FileInputStream(container);
            //reading stored chunk through container
            String chunk = "";
            byte[] buffer = new byte[256];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
//                for (int i = 0; i < len; i++)
//                    chunk += (char)buffer[i];
                chunk += new String(buffer);
            }
            inputStream.close();
            OutputStream outputStream = new FileOutputStream(container, false);
            String[] items = chunk.split(ENDLINE);
            boolean updated = false;
            for (String item :
                    items) {
                if (updated) {
                    outputStream.write((item.getBytes() + ENDLINE).getBytes());
                    continue;
                }
                String[] parts = item.split(KV_SPLITTER);
                if (parts.length != 2) continue;
                byte[] decodedKey = Base64.getDecoder().decode(parts[0].getBytes());
                if (Arrays.equals(key, decodedKey)) {
                    outputStream.write((parts[0].getBytes() + KV_SPLITTER + Base64.getEncoder().encode(value) + ENDLINE).getBytes());
                    updated = true;
                    continue;
                } else {
                    outputStream.write((item.getBytes() + ENDLINE).getBytes());
                    continue;
                }
            }
            outputStream.flush(); outputStream.close();
        }

        private void delete(File container, byte[] key) throws IOException {
            InputStream inputStream = new FileInputStream(container);
            //reading stored chunk through container
            String chunk = "";
            byte[] buffer = new byte[256];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
//                for (int i = 0; i < len; i++)
//                    chunk += (char)buffer[i];
                chunk += new String(buffer);
            }
            inputStream.close();
            boolean containerIsInQueue = false;
            if (chunk.length() < TRASH_HOLD) containerIsInQueue = true;
            OutputStream outputStream = new FileOutputStream(container, false);
            String[] items = chunk.split(ENDLINE);
            boolean deleted = false;
            for (String item :
                    items) {
                if (deleted) {
                    outputStream.write((item.getBytes() + ENDLINE).getBytes());
                    continue;
                }
                String[] parts = item.split(KV_SPLITTER);
                if (parts.length != 2) continue;
                byte[] decodedKey = Base64.getEncoder().encode(parts[0].getBytes());
                if (Arrays.equals(key, decodedKey)) {
                    deleted = true;
                    continue;
                } else {
                    outputStream.write((item.getBytes() + ENDLINE).getBytes());
                    continue;
                }
            }
            outputStream.flush(); outputStream.close();
            if (!containerIsInQueue)
                if (container.length() < TRASH_HOLD) filesQueue.add(container);
        }
    }
}
