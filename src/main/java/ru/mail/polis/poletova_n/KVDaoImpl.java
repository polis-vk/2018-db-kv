package ru.mail.polis.poletova_n;

import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    private final HashMap<ByteArray,byte[]> data;
    public KVDaoImpl(){
        data = new HashMap<>();
    }
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteArray k = new ByteArray(key);
        byte[] res=data.get(k);
        if(res==null){
            throw new NoSuchElementException();
        }
        return res;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteArray k = new ByteArray(key);
        data.put(k,value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteArray k = new ByteArray(key);
        data.remove(k);
    }

    @Override
    public void close() throws IOException {

    }
}
