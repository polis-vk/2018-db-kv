package ru.mail.polis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.NoSuchElementException;

public class KVDaoTest {
    private static KVDao create() throws IOException {
        return KVDaoFactory.create();
    }

    @Test(expected = NoSuchElementException.class)
    public void empty() throws IOException {
        create().get("key".getBytes());
    }

    @Test
    public void insert() throws IOException {
        final KVDao dao = create();
        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();
        dao.upsert(key, value);
        Assert.assertArrayEquals(value, dao.get(key));
        Assert.assertArrayEquals(value, dao.get("key".getBytes()));
    }

    @Test
    public void upsert() throws IOException {
        final KVDao dao = create();
        final byte[] key = "key".getBytes();
        final byte[] value1 = "value1".getBytes();
        final byte[] value2 = "value2".getBytes();
        dao.upsert(key, value1);
        Assert.assertArrayEquals(value1, dao.get(key));
        Assert.assertArrayEquals(value1, dao.get("key".getBytes()));
        dao.upsert(key, value2);
        Assert.assertArrayEquals(value2, dao.get(key));
        Assert.assertArrayEquals(value2, dao.get("key".getBytes()));
    }

    @Test(expected = NoSuchElementException.class)
    public void remove() throws IOException, Exception {
        final KVDao dao = create();
        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();
        dao.upsert(key, value);
        Assert.assertArrayEquals(value, dao.get(key));
        try {
            Assert.assertArrayEquals(value, dao.get("key".getBytes()));
        } catch (NoSuchElementException nSEE) {
            throw new Exception();
        }
        dao.remove(key);
        dao.get(key);
    }
}
