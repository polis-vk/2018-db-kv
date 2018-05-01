package ru.mail.polis.alexantufiev;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Implementation of KVDao.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 01.05.2018
 */
public class KVDaoImpl implements KVDao {

    private File data;

    public KVDaoImpl(File data) {
        this.data = data;
    }

    private String getPath(byte[] key) {
        return data + File.separator + Arrays.toString(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File tempFile = new File(getPath(key));
        //        if (tempFile.exists()) {
        //            return Files.readAllBytes(tempFile.toPath());
        //        } else {
        //            throw new NoSuchElementException("File bot found");
        //        }
        if (!tempFile.exists()) {
            throw new NoSuchElementException("File not found");
        }
        try (FileInputStream fileInputStream = new FileInputStream(tempFile)) {
            byte[] bytes = new byte[fileInputStream.available()];
            if (fileInputStream.read(bytes) < 0) {
                throw new UnsupportedOperationException("Can not read value");
            }
            return bytes;
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(new File(getPath(key)))) {
            fileOutputStream.write(value);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) {
        File tempFile = new File(getPath(key));
        if (!tempFile.delete()) {
            throw new UnsupportedOperationException("Can not be delete");
        }
    }

    @Override
    public void close() {

    }
}
