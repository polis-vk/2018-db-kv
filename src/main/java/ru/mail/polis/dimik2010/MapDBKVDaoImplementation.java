package ru.mail.polis.dimik2010;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.NoSuchElementException;
import org.mapdb.DB;


public class MapDBKVDaoImplementation implements KVDao {
  private static final String NAME_OF_HTREEMAP = "KVDao";
  private static final String FILE_NAME = "Data.db";
  private final DB data;
  private final HTreeMap<byte[], byte[]> dataMap;

  public MapDBKVDaoImplementation(File data) throws IOException {
    if (data.isDirectory()) {
      data = new File(data.getPath().concat("/" + FILE_NAME));
    }
    this.data = DBMaker.fileDB(data.getPath()).fileMmapEnableIfSupported().make();
    this.dataMap = this.data.hashMap(NAME_OF_HTREEMAP)
        .keySerializer(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .createOrOpen();
  }


  @NotNull
  @Override
  public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
    byte[] value;
    if ((value = dataMap.get(key)) == null) {
      throw new NoSuchElementException("NO ELEMENT WITH KEY " + Arrays.toString(key));
    }
    return value;
  }

  @Override
  public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
    this.dataMap.put(key, value);
  }

  @Override
  public void remove(@NotNull byte[] key) throws IOException {
    this.dataMap.remove(key);
  }

  @Override
  public void close() throws IOException {
    data.close();
  }
}
