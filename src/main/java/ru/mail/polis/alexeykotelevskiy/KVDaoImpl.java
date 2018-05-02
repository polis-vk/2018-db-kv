package ru.mail.polis.alexeykotelevskiy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    private BTree<SerializeBuffer, byte[]> bTree;

    public KVDaoImpl(File data) {
        String path = data.getPath() + File.separator + "btree";
        if (Files.exists(Paths.get(path))) {
            bTree = BTree.readFromDisk(path);
        } else {
            bTree = new BTree<>(data.getPath());
        }
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        byte[] val = bTree.search(bKey);
        if (val == null) {
            throw new NoSuchElementException();
        }
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        bTree.add(bKey, value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        SerializeBuffer bKey = new SerializeBuffer(key);
        bTree.remove(bKey);
    }

    @Override
    public void close() throws IOException {
        bTree = null;
    }
}
