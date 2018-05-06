package ru.mail.polis.kirillyarulin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Created by Kirill Yarulin on 05.05.18
 */
public class DiskKVDao implements KVDao {

    private final File directory;

    public DiskKVDao(File directory) {
        this.directory = directory;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File file = new File(directory, Arrays.toString(key));
        if (!file.exists()) {
            throw new NoSuchElementException();
        }

        try (InputStream inputStream = new FileInputStream(file)) {
            return inputStream.readAllBytes();
        }
    }


    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        File file = new File(directory, Arrays.toString(key));

        try (OutputStream inputStream = new FileOutputStream(file)) {
            inputStream.write(value);
        }
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
