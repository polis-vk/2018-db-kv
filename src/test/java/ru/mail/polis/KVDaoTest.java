package ru.mail.polis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
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
    public void nonUnicode() throws IOException {
        // Different byte arrays
        final byte[] a1 = new byte[]{
                (byte) 0xD0, (byte) 0x9F, // 'П'
                (byte) 0xD1, (byte) 0x80, // 'р'
                (byte) 0xD0,              // corrupted UTF-8, was 'и'
                (byte) 0xD0, (byte) 0xB2, // 'в'
                (byte) 0xD0, (byte) 0xB5, // 'е'
                (byte) 0xD1, (byte) 0x82  // 'т'
        };
        final byte[] a2 = new byte[]{
                (byte) 0xD0, (byte) 0x9F, // 'П'
                (byte) 0xD1, (byte) 0x80, // 'р'
                (byte) 0xD1,              // corrupted UTF-8, was 'и'
                (byte) 0xD0, (byte) 0xB2, // 'в'
                (byte) 0xD0, (byte) 0xB5, // 'е'
                (byte) 0xD1, (byte) 0x82  // 'т'
        };
        Assert.assertFalse(Arrays.equals(a1, a2));

        // But same strings
        Assert.assertArrayEquals(new String(a1).getBytes(), new String(a2).getBytes());

        final KVDao dao = create();

        // Put a1 value
        final byte[] value = "value".getBytes();
        dao.upsert(a1, value);

        // Check that a2 value is absent
        try {
            dao.get(a2);
            Assert.fail();
        } catch (NoSuchElementException e) {
            // OK
        }
    }

    @Test
    public void nonHash() throws IOException {
        // Different byte arrays
        final byte[] a1 = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1F};
        final byte[] a2 = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00};
        Assert.assertFalse(Arrays.equals(a1, a2));

        // But hash codes are equal
        Assert.assertEquals(Arrays.hashCode(a1), Arrays.hashCode(a2));

        final KVDao dao = create();

        // Put a1 value
        final byte[] value = "value".getBytes();
        dao.upsert(a1, value);

        // Check that a2 value is absent
        try {
            dao.get(a2);
            Assert.fail();
        } catch (NoSuchElementException e) {
            // OK
        }
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