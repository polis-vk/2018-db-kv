package ru.mail.polis.poletova_n;

import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    HashMap<ByteArray,byte[]> data;
    public KVDaoImpl(){
        data = new HashMap<>();
    }
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        ByteArray k = new ByteArray(key);
        if(data.get(k)==null){
            throw new NoSuchElementException();
        }
        return data.get(k);
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        ByteArray k = new ByteArray(key);
        if(data.containsKey(k)){
            data.replace(k,value);
        } else {
            data.put(k,value);
        }
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
