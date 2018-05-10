package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class KVDaoImplPersistence implements KVDao {
    private static final int MEMTABLE_MAX_SIZE = 65536;
    private static final String SSTABLE_FILE_NAME = "sstable";

    private final Map<ByteBuffer, Long> ssTable = new HashMap<>();
    private final Map<ByteBuffer, byte[]> memTable = new HashMap<>();
    private final File dir;

    private int memTableSize = 0;

    public KVDaoImplPersistence(@NotNull File dir) throws IOException {
        if (!dir.exists()) dir.mkdir();
        this.dir = dir;
        buildSSTable();
    }

    private void buildSSTable() throws IOException {
        File file = new File(dir + File.separator + SSTABLE_FILE_NAME);
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            while (in.available() > 0) {
                int keyLength = in.readInt();
                byte[] currKey = new byte[keyLength];
                in.read(currKey, 0, keyLength);
                long fileName = in.readLong();
                ssTable.put(ByteBuffer.wrap(currKey), fileName);
            }
        } catch (FileNotFoundException ignored) {
            // if there is no sstable - that's new database
        }
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key);
        byte[] result = memTable.get(wrappedKey);
        Long fileWithKey = ssTable.get(wrappedKey);

        if (result == null && fileWithKey != null) {
            File file = new File(dir + File.separator + fileWithKey);
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                while (in.available() > 0) {
                    int keyLength = in.readInt();

                    byte[] currKey = new byte[keyLength];
                    in.read(currKey, 0, keyLength);

                    int valueLength = in.readInt();

                    if (Arrays.equals(currKey, key)) {
                        byte[] value = new byte[valueLength];
                        in.read(value, 0, valueLength);
                        result = value;
                        break;
                    } else {
                        in.skipBytes(valueLength);
                    }
                }
            }
        }

        if (result == null) {
            throw new NoSuchElementException();
        }

        return result;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key);
        memTable.put(wrappedKey, value);
        memTableSize += key.length + value.length;

        if (memTableSize > MEMTABLE_MAX_SIZE) {
            saveMemtable();
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key);
        byte[] value = memTable.remove(wrappedKey);

        if (value != null) {
            memTableSize -= key.length + value.length;
        }
    }

    @Override
    public void close() throws IOException {
        saveMemtable();
        saveSStable();
    }

    private void saveMemtable() throws IOException {
        final long timestamp = System.nanoTime();
        final File file = new File(dir + File.separator + timestamp);

        try (DataOutputStream out =
                     new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            for (Map.Entry<ByteBuffer, byte[]> entry : memTable.entrySet()) {
                byte[] key = entry.getKey().array();
                byte[] value = entry.getValue();

                if (value != null) {
                    out.writeInt(key.length);
                    out.write(key);

                    out.writeInt(value.length);
                    out.write(value);

                    ssTable.put(entry.getKey(), timestamp);
                }
            }
        }

        memTableSize = 0;
        memTable.clear();
    }

    private void saveSStable() throws IOException {
        final File ssTableFile = new File(dir + File.separator + SSTABLE_FILE_NAME);
        try (DataOutputStream out =
                     new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ssTableFile, false)))) {
            for (Map.Entry<ByteBuffer, Long> entry : ssTable.entrySet()) {
                byte[] key = entry.getKey().array();
                Long fileName = entry.getValue();

                out.writeInt(key.length);
                out.write(key);
                out.writeLong(fileName);
            }
        }
    }
}
