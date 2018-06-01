package ru.mail.polis.zhenytim;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final File directory;

    public KVDaoImpl(File directory) {
        this.directory = directory;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File f = new File(directory, Arrays.toString(key));
        if (!f.exists()) {
            throw new NoSuchElementException();
        }

        try (InputStream inStream = new FileInputStream(f)) {
            byte[] result = new byte[inStream.available()];
            inStream.read(result);
            return result;
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        File file = new File(directory, Arrays.toString(key));
        OutputStream inStream = new FileOutputStream(file);
        inStream.write(value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        File file = new File(directory, Arrays.toString(key));
        file.delete();
    }

    @Override
    public void close() throws IOException {

    }
}
