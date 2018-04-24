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

import com.sun.org.apache.xerces.internal.impl.io.MalformedByteSequenceException;
import org.jetbrains.annotations.NotNull;
import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileVisitResult;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.BiConsumer;

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
        final private Map<ByteBuffer, Long> storage = new HashMap<>();;
        final private Queue<Long> filesQueue = new LinkedList<>();

        private Long fileNumber = new Long(0);

        /*
        * Chunk size in kbytes
        */
        public KVDaoImpl(final File dir, final int chunkSize) {
            this.STORAGE_DIR = dir + File.separator;
            this.TRASH_HOLD = chunkSize * KBYTE;
            try {
                java.nio.file.Files.walkFileTree(
                        dir.toPath(),
                        new SimpleFileVisitor<Path>() {
                            private void fetchData(@NotNull final Path file) throws IOException {
                                Long srcLong = Long.parseLong(file.toString());
                                if (srcLong >= fileNumber) fileNumber = new Long(srcLong+1);
                                new FileHolder(file.toFile()).forEach((byteBuffer, bytes) -> {
                                    storage.put(byteBuffer, srcLong);
                                });
                            }

                            @NotNull
                            @Override
                            public FileVisitResult visitFile(@NotNull final Path file, @NotNull final BasicFileAttributes attrs)
                                    throws IOException {
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
        public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {
            final byte[] bytes = new FileHolder(new File(STORAGE_DIR + this.storage.get(ByteBuffer.wrap(key)).toString())).get(key);
            if (bytes == null) throw new NoSuchElementException();
            return bytes;
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            Long distLong = this.storage.get(ByteBuffer.wrap(key));
            if (distLong == null) {
                Long containerCandidate = getContainer();
                if (containerCandidate == null) {
                    distLong = this.fileNumber++;
                    File distFile = new File(STORAGE_DIR + Long.toString(distLong));
                    distFile.createNewFile();
                    new FileHolder(distFile).upsert(key, value);
                    this.storage.put(ByteBuffer.wrap(key), distLong);
                    if (distFile.length() < TRASH_HOLD) filesQueue.add(distLong);
                } else {
                    File distFile = new File(STORAGE_DIR + Long.toString(containerCandidate));
                    new FileHolder(distFile).upsert(key, value);
                    this.storage.put(ByteBuffer.wrap(key), distLong);
                    if (distFile.length() >= TRASH_HOLD) filesQueue.remove();
                }
            } else {
                File distFile = new File(STORAGE_DIR + Long.toString(distLong));
                if (!distFile.canRead() || !distFile.exists()) throw new IOException();
                new FileHolder(distFile).upsert(key, value);
            }
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException, NoSuchElementException {
            File dist = new File(STORAGE_DIR + this.storage.get(ByteBuffer.wrap(key)).toString());
            if (!dist.exists() || !dist.canRead()) throw new IOException();
            new FileHolder(dist).remove(key);
        }

        @Override
        public void close() throws IOException {

        }

        private Long getContainer(){
            while(true) {
                if (this.filesQueue.size() == 0) return null;
                Long candidate = this.filesQueue.peek();
                if (new File(candidate.toString()).length() < TRASH_HOLD) return candidate;
                else this.filesQueue.remove();
            }
        }
    }

    private static class FileHolder {
        private final long MIN_FILE_LENGTH = Integer.BYTES * 2 + Byte.BYTES * 2;

        private final File source;
        private long size;
        private Map<ByteBuffer, byte[]>map;

        public FileHolder(final File src) throws StreamCorruptedException, FileNotFoundException, IOException{
            if (!src.exists()) throw new FileNotFoundException();
            else if (!src.isFile() || !src.canRead()) throw new IOException();
            if (src.length() == 0) {
                this.source = src;
                this.size = src.length();
                this.map = new LinkedHashMap();
            } else {
                if (src.length() >= MIN_FILE_LENGTH) {
                    this.source = src;
                    this.size = src.length();
                    InputStream inputStream = new FileInputStream(this.source);
                    long index = -1;
                    while (index <= this.size - 1) {
                        byte[] bytes = new byte[Integer.BYTES];
                        if (inputStream.read(bytes) != Integer.BYTES) throw new IOException();
                        index += Integer.BYTES;
                        int keyLength = getInt(bytes);
                        if (inputStream.read(bytes) != Integer.BYTES) throw new IOException();
                        index += Integer.BYTES;
                        int valueLength = getInt(bytes);
                        bytes = null;
                        byte[] key = new byte[keyLength];
                        if (inputStream.read(key) != keyLength) throw new IOException();
                        index += keyLength;
                        byte[] value = new byte[valueLength];
                        if (inputStream.read(value) != valueLength) throw new IOException();
                        index += valueLength;
                        this.map.put(ByteBuffer.wrap(bytes), value);
                    }
                    inputStream.close();
                } else throw new StreamCorruptedException();
            }
        }

        public byte[] get(byte[] key) {
            return this.map.get(ByteBuffer.wrap(key));
        }

        public void upsert(byte[] key, byte[] value) throws IOException{
            this.map.put(ByteBuffer.wrap(key), value);
            flush();
        }

        public void remove(byte[] key) throws IOException, NoSuchElementException{
            if (this.map.remove(ByteBuffer.wrap(key)) == null) throw new NoSuchElementException();
            flush();
        }

        public void forEach(BiConsumer<ByteBuffer, byte[]> consumer) {
            this.map.forEach(consumer);
        }

        private void flush() throws IOException{
            final OutputStream outputStream = new FileOutputStream(source, false);
            try {
                this.map.forEach((byteBuffer, bytes) -> {
                    try {
                        outputStream.write(byteBuffer.capacity());
                        outputStream.write(bytes.length);
                        outputStream.write(byteBuffer.get());
                        outputStream.write(bytes);
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                });
            } finally {
                outputStream.flush();
                outputStream.close();
            }
        }

        private static int getInt(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            return byteBuffer.getInt(0);
        }
    }

}
