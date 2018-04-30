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
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }

        return new KVDaoImpl(data);
    }

    private static class KVDaoImpl implements KVDao {
        final private int MEM_TABLE_TRASH_HOLD = 1024 * 20;
        final private String STORAGE_DIR;

        final private SortedMap<ByteBuffer, byte[]> memTable = new TreeMap<>(new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer o1, ByteBuffer o2) {
                return o1.compareTo(o2);
            }
        });

        final private SnapshotHolder holder;

        private Long memTablesize = 0L;

        public KVDaoImpl(final File dir) {
            this.STORAGE_DIR = dir + File.separator;
            this.holder = new SnapshotHolder(this.STORAGE_DIR);
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws IOException, NoSuchElementException {
            byte[] value = this.memTable.get(ByteBuffer.wrap(key));
            if (value == null) {
                return this.holder.get(key);
            } else {
                if (value == SnapshotHolder.REMOVED_VALUE) {
                    throw new NoSuchElementException();
                } else {
                    return value;
                }
            }
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            this.memTable.put(ByteBuffer.wrap(key), value);
            this.memTablesize += key.length + value.length;
            if (memTablesize >= MEM_TABLE_TRASH_HOLD) {
                this.holder.save(this.memTable);
                this.memTable.clear();
                this.memTablesize = 0L;
            }
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException, NoSuchElementException {
            byte[] value = this.memTable.get(ByteBuffer.wrap(key));
            if (value != null) {
                if (value == SnapshotHolder.REMOVED_VALUE) {
                    throw new NoSuchElementException();
                } else {
                    this.memTable.put(ByteBuffer.wrap(key), SnapshotHolder.REMOVED_VALUE);
                }
            } else {
                if (!this.holder.contains(key)) {
                    throw new NoSuchElementException();
                } else {
                    this.memTable.put(ByteBuffer.wrap(key), SnapshotHolder.REMOVED_VALUE);
                }
            }
        }

        @Override
        public void close() throws IOException {
            this.holder.save(this.memTable);
        }
    }

    private static class SnapshotHolder {
        final public static byte[] REMOVED_VALUE = new byte[0];

        final private Map<ByteBuffer, Long> sSMap = new HashMap<>();
        final private File storage;

        private Long fileNumber = 0L;

        public SnapshotHolder(String dir) /*throws IOException*/{
            this.storage = new File(dir);
//            if (!this.storage.exists()) throw new IOException();
            HashMap<ByteBuffer, Long> timeStamps = new HashMap<>();
            try {
                java.nio.file.Files.walkFileTree(
                        this.storage.toPath(),
                        new SimpleFileVisitor<Path>() {
                            private void fetchData(@NotNull final Path file) throws IOException {
                                RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r");
                                long currentTStamp = randomAccessFile.readLong();
                                int amount = randomAccessFile.readInt();
                                for (int i = 0; i < amount; i++) {
                                    int length = randomAccessFile.readInt();
                                    byte[] bytes = new byte[length];
                                    randomAccessFile.read(bytes);
                                    int offset = randomAccessFile.readInt();
                                    ByteBuffer keyBuffer = ByteBuffer.wrap(bytes);
                                    if (offset == -1) {
                                        Long itemTStamp = timeStamps.get(keyBuffer);
                                        if (itemTStamp == null) {
                                            timeStamps.put(keyBuffer, currentTStamp);
                                        } else {
                                            if (itemTStamp < currentTStamp) {
                                                sSMap.remove(keyBuffer);
                                                timeStamps.put(keyBuffer, currentTStamp);
                                            }
                                        }
                                    } else {
                                        Long itemTStamp = timeStamps.get(keyBuffer);
                                        if (itemTStamp == null) {
                                            timeStamps.put(keyBuffer, currentTStamp);
                                            sSMap.put(keyBuffer, Long.parseLong(file.toFile().getName()));
                                        } else {
                                            if (itemTStamp < currentTStamp) {
                                                sSMap.put(keyBuffer, Long.parseLong(file.toFile().getName()));
                                                timeStamps.put(keyBuffer, currentTStamp);
                                            }
                                        }
                                    }
                                }
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
            }
        }

        public byte[] get(byte[] key) throws IOException, NoSuchElementException {
            Long index = this.sSMap.get(ByteBuffer.wrap(key));
            if (index == null) throw new NoSuchElementException();
            File source = new File(this.storage.toString() + File.separator + index.toString());
            if (!source.canRead() || !source.exists() || !source.isFile()) throw new IOException();
            RandomAccessFile randomAccessFile = new RandomAccessFile(source, "r");
            randomAccessFile.skipBytes(Long.BYTES);
            int amount = randomAccessFile.readInt();
            ArrayList<Key> keys = new ArrayList<>();
            for (int i = 0 ; i < amount; i++) {
                Key storedKey = new Key();
                storedKey.setLength(randomAccessFile.readInt());
                byte[] bytes = new byte[storedKey.getLength()];
                randomAccessFile.read(bytes);
                storedKey.setKey(bytes);
                storedKey.setOffset(randomAccessFile.readInt());
                if (storedKey.getOffset() != -1)
                    keys.add(storedKey);
            }
            Integer valueOffset = null;
            for (Key k :
                    keys) {
                if (Arrays.equals(k.getKey(), key)) {
                    valueOffset = k.getOffset();
                    break;
                }
            }
            if (valueOffset == null) throw new NoSuchElementException();
            randomAccessFile.skipBytes(valueOffset);
            int valueLength = randomAccessFile.readInt();
            byte[] value = new byte[valueLength];
            randomAccessFile.read(value);
            randomAccessFile.close();
            return value;
        }

        public boolean contains(byte[] key) {
            return this.sSMap.containsKey(ByteBuffer.wrap(key));
        }

        public void save(SortedMap<ByteBuffer, byte[]> source) throws IOException {
            int offset = 0;
            File dist = new File(this.storage + File.separator + fileNumber.toString());
            dist.createNewFile();

            ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
            OutputStream outputStream = new FileOutputStream(dist);
            outputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());
            outputStream.write(intBuffer.putInt(source.size()).array()); intBuffer.clear();

            for (Map.Entry<ByteBuffer, byte[]> entry : source.entrySet()) {
                outputStream.write(intBuffer.putInt(entry.getKey().array().length).array()); intBuffer.clear();
                outputStream.write(entry.getKey().array());
                outputStream.write(intBuffer.putInt(offset).array()); intBuffer.clear();
                offset += Integer.BYTES + entry.getValue().length;
            }

            for (Map.Entry<ByteBuffer, byte[]> entry : source.entrySet()) {
                outputStream.write(intBuffer.putInt(entry.getValue().length).array()); intBuffer.clear();
                outputStream.write(entry.getValue());
            }
            outputStream.flush();
            outputStream.close();
            for (Map.Entry<ByteBuffer, byte[]> entry : source.entrySet()) {
                this.sSMap.put(entry.getKey(), fileNumber);
            }
            fileNumber++;
        }

        private int getInt(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
            return byteBuffer.getInt(0);
        }

        private class Key {
            private int length;
            private byte[] key;
            private int offset;

            public Key(){};

            public Key(int length, byte[] key, int offset) {
                this.length = length;
                this.key = key;
                this.offset = offset;
            }

            public int getLength() {
                return length;
            }

            public void setLength(int length) {
                this.length = length;
            }

            public byte[] getKey() {
                return key;
            }

            public void setKey(byte[] key) {
                this.key = key;
            }

            public int getOffset() {
                return offset;
            }

            public void setOffset(int offset) {
                this.offset = offset;
            }
        }
    }

//    private static class FileHolder {
//        private final long MIN_FILE_LENGTH = Integer.BYTES * 2 + Byte.BYTES * 2;
//
//        private final File source;
//        private final Long sourceNameAsLong;
//        private long size;
//        private Map<ByteBuffer, Value[]>map;
//
//        public FileHolder(final File src) throws StreamCorruptedException, FileNotFoundException, IOException {
//            if (!src.exists()) throw new FileNotFoundException();
//            else if (!src.isFile() || !src.canRead()) throw new IOException();
//            this.source = src;
//            this.size = src.length();
//            this.sourceNameAsLong = Long.parseLong(this.source.getName());
//            this.map = new LinkedHashMap();
//            if (src.length() != 0) {
//                if (src.length() >= MIN_FILE_LENGTH) {
//                    InputStream inputStream = new FileInputStream(this.source);
//                    long index = -1;
//                    while (index < this.size - 1) {
//                        Value[] bytes = new byte[Integer.BYTES];
//                        if (inputStream.read(bytes) != Integer.BYTES) throw new IOException();
//                        index += Integer.BYTES;
//                        int keyLength = getInt(bytes);
//                        if (inputStream.read(bytes) != Integer.BYTES) throw new IOException();
//                        index += Integer.BYTES;
//                        int valueLength = getInt(bytes);
//                        bytes = new byte[keyLength];
//                        if (inputStream.read(bytes) != keyLength) throw new IOException();
//                        final ByteBuffer keyBuffer = ByteBuffer.wrap(bytes);
//                        index += keyLength;
//                        bytes = new byte[valueLength];
//                        if (valueLength != 0)
//                            if (inputStream.read(bytes) != valueLength) throw new IOException();
//                        index += valueLength;
//                        this.map.put(keyBuffer,
//                                bytes);
//                    }
//                    inputStream.close();
//                } else
//                    throw new StreamCorruptedException();
//            }
//        }
//
//        public Long getName() {
//            return sourceNameAsLong;
//        }
//
//        public Value[] get(Value[] key) {
//                Value[] bytes = this.map.get(ByteBuffer.wrap(key));
//                return bytes;
//        }
//
//        public void upsert(Value[] key, Value[] value) throws IOException{
//            this.map.put(ByteBuffer.wrap(key), value);
//            flush();
//        }
//
//        public void remove(Value[] key) throws IOException, NoSuchElementException{
//            if (this.map.remove(ByteBuffer.wrap(key)) == null) throw new NoSuchElementException();
//            flush();
//        }
//
//        public void forEach(BiConsumer<ByteBuffer, Value[]> consumer) {
//            this.map.forEach(consumer);
//        }
//
//        private void flush() throws IOException{
//            final OutputStream outputStream = new FileOutputStream(source, false);
//            try {
//                this.map.forEach((byteBuffer, bytes) -> {
//                    try {
//                        outputStream.write(ByteBuffer.allocate(Integer.BYTES).putInt(byteBuffer.capacity()).array());
//                        outputStream.write(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
//                        outputStream.write(byteBuffer.array());
//                        outputStream.write(bytes);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                        System.exit(1);
//                    }
//                });
//            } finally {
//                outputStream.flush();
//                outputStream.close();
//            }
//        }
//}
}
