package ru.mail.polis.shnus;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {


    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        return new byte[0];
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {

    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
