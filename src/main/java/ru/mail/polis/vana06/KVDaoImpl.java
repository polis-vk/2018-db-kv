package ru.mail.polis.vana06;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private final Environment env;
    private final Store store;
    private Transaction txn;

    private double threshold = 0.25;

    public KVDaoImpl(File data) throws IOException{
        env = Environments.newInstance(data);
        store = env.computeInTransaction(txn -> env.openStore("MyStore", StoreConfig.WITHOUT_DUPLICATES, txn));
        txn = env.beginTransaction();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        try {
            return store.get(txn, new ArrayByteIterable(key)).getBytesUnsafe();
        } catch (NullPointerException e){
            throw new NoSuchElementException();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        if(Runtime.getRuntime().freeMemory() < Runtime.getRuntime().maxMemory()*threshold){
            txn.commit();
            txn = env.beginTransaction();
        }
        store.put(txn, new ArrayByteIterable(key), new ArrayByteIterable(value));
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        store.delete(txn, new ArrayByteIterable(key));
    }

    @Override
    public void close() throws IOException {
        txn.commit();
        env.close();
    }

}
