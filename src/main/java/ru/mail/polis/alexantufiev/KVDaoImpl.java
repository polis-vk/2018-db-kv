package ru.mail.polis.alexantufiev;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;

/**
 * Implementation of KVDao.
 *
 * @author Aleksey Antufev
 * @version 1.1.0
 * @since 1.1.0 01.05.2018
 */
public class KVDaoImpl implements KVDao {

    private final Environment environment;
    private static final String STORAGE_NAME = "MyStorage";

    public KVDaoImpl(@NotNull File data) {
        environment = Environments.newInstance(data);
    }

    private ByteIterable bytesToEntry(@NotNull byte[] bytes) {
        return new ArrayByteIterable(bytes);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        final ByteIterable[] byteIterable = new ByteIterable[1];
        environment.executeInTransaction(txn -> byteIterable[0] = getStore(txn).get(txn, bytesToEntry(key)));

        if (byteIterable[0] == null) {
            throw new NoSuchElementException("File not found");
        } else {
            return byteIterable[0].getBytesUnsafe();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        environment.executeInTransaction(txn -> getStore(txn).put(
            txn,
            bytesToEntry(key),
            bytesToEntry(value)
        ));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        environment.executeInTransaction(txn -> getStore(txn).delete(txn, bytesToEntry(key)));
    }

    @NotNull
    private Store getStore(Transaction txn) {
        return environment.openStore(STORAGE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn);
    }

    @Override
    public void close() {
        environment.close();
    }
}
