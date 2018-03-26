package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Key-value DAO API
 */
public interface KVDao extends Closeable {
    @NotNull
    byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException;

    void upsert(
            @NotNull byte[] key,
            @NotNull byte[] value) throws IOException;

    void remove(@NotNull byte[] key) throws IOException;
}
