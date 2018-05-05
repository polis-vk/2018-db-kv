package ru.mail.polis.poletova_n;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class KVDaoImpl implements KVDao {

    private final HashMap<ByteArray,byte[]> data;
    private File file;
    public KVDaoImpl(File file){
        data = new HashMap<>();
        this.file = file;
        File[] list = file.listFiles();
        for(File i:list){
            if(i.isFile()){
                String key = i.getName();
                Scanner scan = new Scanner(key);
                if(scan.hasNext()) {
                    data.put(new ByteArray(key.getBytes()), scan.nextLine().getBytes());
                }
                scan.close();
            }

        }

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
        Integer i =  ByteBuffer.wrap(key).hashCode();
        Integer ii= ByteBuffer.wrap(value).hashCode();
        File f = new File(file.getAbsolutePath(),i.toString());
        if(f.exists()) {
            FileWriter fw = new FileWriter(f,false);
            fw.write(ii.toString());
            fw.close();
        } else {
            boolean bool = f.createNewFile();
            if (bool) {
                FileWriter fw = new FileWriter(f, false);
                fw.write(ii.toString());
                fw.close();
            }
        }

        data.put(k,value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        ByteArray k = new ByteArray(key);
        Integer i =  ByteBuffer.wrap(key).hashCode();
        File f = new File(file.getAbsolutePath(),toString());
        f.delete();
        data.remove(k);
    }

    @Override
    public void close() throws IOException {

    }
}
