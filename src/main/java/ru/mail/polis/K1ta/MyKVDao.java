package ru.mail.polis.K1ta;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
        File _file = new File(file + "//" + Arrays.toString(_key.array()));
        if (!_file.exists()) {
            throw new NoSuchElementException();
        }
        FileInputStream f = null;
        try {
            f = new FileInputStream(_file);
            byte[] val = new byte[f.available()];
            f.read(val);
            return val;
        } finally {
            f.close();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteBuffer _key = ByteBuffer.wrap(key);
        map.put(_key, value);
        File _file = new File(file + "//" + Arrays.toString(_key.array()));
        _file.createNewFile();
        FileOutputStream f = null;
        try {
            f = new FileOutputStream(_file);
            f.write(value);
        } finally {
            f.close();
        }
    }

    @Override
    public void remove(@NotNull byte[] key) {
        ByteBuffer _key = ByteBuffer.wrap(key);
        File _file = new File(file + "//" + Arrays.toString(_key.array()));
        if (_file.exists()) {
            _file.delete();
        }
    }

    @Override
    public void close() {
        map.clear();
    }
}
