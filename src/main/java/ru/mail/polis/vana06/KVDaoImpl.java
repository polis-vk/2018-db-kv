package ru.mail.polis.vana06;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Map<ByteArrayWrapper, byte[]> map = new HashMap<>();
    private final Environment env;
    private final Store store;

    public KVDaoImpl(File data) {
        env = Environments.newInstance(data);
        store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull final Transaction txn) {
                return env.openStore("MyStore", StoreConfig.WITHOUT_DUPLICATES, txn);
            }
        });
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        final ByteIterable keyToGet = new ArrayByteIterable(key);
        Transaction txn = env.beginReadonlyTransaction();
        try {
            return store.get(txn, keyToGet).getBytesUnsafe();
        } catch (NullPointerException e){
            throw new NoSuchElementException();
        } finally {
            txn.abort();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        final ByteIterable keyToSave = new ArrayByteIterable(key);
        final ByteIterable valueToSave = new ArrayByteIterable(value);
        Transaction txn = env.beginTransaction();
        try {
            store.put(txn, keyToSave, valueToSave);
        } finally {
            txn.commit();
        }


    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        final ByteIterable keyToDelete = new ArrayByteIterable(key);
        Transaction txn = env.beginTransaction();
        try {
            store.delete(txn, keyToDelete);
        } finally {
            txn.commit();
        }
    }

    @Override
    public void close() throws IOException {
        env.close();
    }
}
