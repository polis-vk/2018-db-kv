package ru.mail.polis.alexeykotelevskiy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    private BTree<Integer, byte[]> bTree = new BTree<>();
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        Integer bKey = Arrays.hashCode(key);
        byte[] val = bTree.search(bKey);
        if (val == null)
        {
            throw new NoSuchElementException();
        }
       return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        Integer bKey = Arrays.hashCode(key);
        bTree.add(bKey, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        Integer bKey = Arrays.hashCode(key);
        bTree.remove(bKey);
    }

    @Override
    public void close() throws IOException {

    }
}
