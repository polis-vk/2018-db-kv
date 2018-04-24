/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Functional unit tests for {@link KVDao} implementations
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public class KVDaoTest extends TestBase {
    private static File data;
    private static KVDao dao;

    @BeforeClass
    public static void beforeAll() throws IOException {
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
    }

    @AfterClass
    public static void afterAll() throws IOException {
        dao.close();
        Files.recursiveDelete(data);
    }

    @Test(expected = NoSuchElementException.class)
    public void empty() throws IOException {
        dao.get(randomKey());
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

        // Put a1 value
        final byte[] value = randomValue();
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

        // Put a1 value
        final byte[] value = randomValue();
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
        final byte[] key = randomKey();
        final byte[] value = randomValue();
        dao.upsert(key, value);
        Assert.assertArrayEquals(value, dao.get(key));
        Assert.assertArrayEquals(value, dao.get(key.clone()));
    }

    @Test
    public void emptyValue() throws IOException {
        final byte[] key = randomKey();
        final byte[] value = new byte[0];
        dao.upsert(key, value);
        Assert.assertArrayEquals(value, dao.get(key));
        Assert.assertArrayEquals(value, dao.get(key.clone()));
    }

    @Test
    public void upsert() throws IOException {
        final byte[] key = randomKey();
        final byte[] value1 = randomValue();
        final byte[] value2 = randomValue();
        dao.upsert(key, value1);
        Assert.assertArrayEquals(value1, dao.get(key));
        Assert.assertArrayEquals(value1, dao.get(key.clone()));
        dao.upsert(key, value2);
        Assert.assertArrayEquals(value2, dao.get(key));
        Assert.assertArrayEquals(value2, dao.get(key.clone()));
    }

    @Test(expected = NoSuchElementException.class)
    public void remove() throws IOException, Exception {
        final byte[] key = randomKey();
        final byte[] value = randomValue();
        dao.upsert(key, value);
        Assert.assertArrayEquals(value, dao.get(key));
        try {
            Assert.assertArrayEquals(value, dao.get(key.clone()));
        } catch (NoSuchElementException nSEE) {
            throw new Exception();
        }
        dao.remove(key);
        dao.get(key);
    }
}
