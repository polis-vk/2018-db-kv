package ru.mail.polis.K1ta;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.NoSuchElementException;

public class MyKVDao implements KVDao {
    HashMap<ByteBuffer, byte[]> map = new HashMap<>();
    File file;

    public MyKVDao(File data) {
        this.file = data;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        File _file = new File(file + "//" + _key.hashCode());
        if (!_file.exists()) {
            throw new NoSuchElementException();
        }
        FileInputStream f = new FileInputStream(_file);
        byte[] val = f.readAllBytes();
        f.close();
        return val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        File _file = new File(file + "//" + _key.hashCode());
        _file.createNewFile();
        FileOutputStream f = new FileOutputStream(_file);
        f.write(value);
        f.close();
    }

    @Override
    public void remove(@NotNull byte[] key) {
        ByteBuffer _key = ByteBuffer.wrap(key);
        File _file = new File(file + "//" + _key.hashCode());
        if (_file.exists()) {
            _file.delete();
        }
    }

    @Override
    public void close() {
        map.clear();
    }
}
