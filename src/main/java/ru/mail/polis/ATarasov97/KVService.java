package ru.mail.polis.ATarasov97;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;


import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import ru.mail.polis.KVDao;

public class KVService implements KVDao {

    private final Environment env;
    private final Map<ByteBuffer, byte[]> data = new HashMap<>();

    public KVService(@NotNull final File data) {
        env = Environments.newInstance(data);
    }
    private ByteIterable wrap(byte[] key) {
        return new ArrayByteIterable(key);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final ByteIterable[] result = new ByteIterable[1];
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final Store store = env.openStore("ServiceStore", StoreConfig.WITHOUT_DUPLICATES, txn);
                result[0] = store.get(txn, wrap(key));
            }
        });
        if (result[0] == null) {
            throw new NoSuchElementException("No element with key:" + Arrays.toString(key));
        }
        return result[0].getBytesUnsafe();
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final Store store = env.openStore("ServiceStore", StoreConfig.WITHOUT_DUPLICATES, txn);
                store.put(txn, wrap(key), wrap(value));
            }
        });
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final Store store = env.openStore("ServiceStore", StoreConfig.WITHOUT_DUPLICATES, txn);
                store.delete(txn, wrap(key));
            }
        });
    }

    @Override
    public void close() throws IOException {
        env.close();
    }
}
