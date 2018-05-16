package ru.mail.polis.K1ta;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class MyKVDao implements KVDao {
    private File file;

    public MyKVDao(File data) {
        this.file = data;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File _file = createFile(key);
        if (!_file.exists()) {
            throw new NoSuchElementException();
        }
        return Files.readAllBytes(_file.toPath());
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        File _file = createFile(key);
        Files.write(_file.toPath(), value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        File _file = createFile(key);
        _file.delete();
    }

    @Override
    public void close() {
    }

    private File createFile(byte[] key) {
        return new File(file, Arrays.toString(key));
    }
}
