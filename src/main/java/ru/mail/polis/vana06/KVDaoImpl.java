package ru.mail.polis.vana06;

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
    private final File data;

    public KVDaoImpl(File data) {
        this.data = data;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] value = map.get(new ByteArrayWrapper(key));
        if(value == null){
            File file = new File(data.toPath() + "//" + new ByteArrayWrapper(key).hashCode());
            if(!file.exists() || !file.isFile()){
                throw new NoSuchElementException();
            }
            value = Files.readAllBytes(file.toPath());
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        map.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        byte[] value = map.remove(new ByteArrayWrapper(key));
        if(value == null) {
            File file = new File(data.toPath() + "//" + new ByteArrayWrapper(key).hashCode());
            file.delete();
        }
    }

    @Override
    public void close() throws IOException {
        for(Map.Entry<ByteArrayWrapper, byte[]> entry: map.entrySet()){
            File newFile = new File(data.toPath() + "//" + entry.getKey().hashCode());
            Files.write(newFile.toPath(), entry.getValue());
        }
        map.clear();
    }
}
