package ru.mail.polis.DaoImpl;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;

import org.rocksdb.RocksDBException;
import ru.mail.polis.KVDao;

    public class DaoRocks implements KVDao {


        private RocksDB db;

        public DaoRocks(@NotNull File data) {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            try {
                db = RocksDB.open(options, data.getPath());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @NotNull
        @Override
        public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
            byte[] bytes = new byte[0];
            try {
                bytes = db.get(key);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
            if (bytes == null) {
                throw new NoSuchElementException();
            }
            return bytes;
        }

        @Override
        public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
            try {
                db.put(key, value);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void remove(@NotNull byte[] key) throws IOException {
            try {
                db.remove(key);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
            db.close();
        }
    }
