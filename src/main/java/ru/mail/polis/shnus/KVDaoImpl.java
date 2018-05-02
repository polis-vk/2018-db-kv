package ru.mail.polis.shnus;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final File path;

    public KVDaoImpl(File path) {
        this.path = path;
    }

    private String getFilePath(byte[] key) {
        return path + File.separator + Arrays.toString(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File file = new File(getFilePath(key));
        if (file.isFile()) {
            return Files.readAllBytes(file.toPath());
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try (OutputStream os = new FileOutputStream(getFilePath(key), false)) {
            os.write(value);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        File file = new File(getFilePath(key));

        if (!file.isFile()) {
            return;
        }

        if (file.delete()) {
            //successfully
        } else {
            //denied Should wait and try again? Or return false?
        }
    }

    @Override
    public void close() throws IOException {

    }
}
