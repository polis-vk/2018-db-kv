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

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;

/**
 * Load tests for the storage
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public class LoadTest {
    private static byte[] keyFrom(final long i) {
        return BigInteger.valueOf(i).multiply(BigInteger.valueOf(13)).toByteArray();
    }

    private static BigInteger initial() {
        final byte[] seed = new byte[8 * 1024];
        ThreadLocalRandom.current().nextBytes(seed);
        return new BigInteger(seed);
    }

    private static BigInteger next(final BigInteger current) {
        return current.add(BigInteger.ONE);
    }

    @Ignore("Per aspera ad astra")
    @Test
    public void bulkInsert() throws IOException {
        final int keys = 100_000;
        final BigInteger initial = initial();

        final File data = Files.createTempDirectory();
        KVDao dao = KVDaoFactory.create(data);
        try {
            // Fill the storage
            BigInteger value = initial;
            for (int i = 0; i < keys; i++) {
                dao.upsert(keyFrom(i), value.toByteArray());
                value = next(value);
            }

            // Reopen
            dao.close();
            dao = KVDaoFactory.create(data);

            // Check the storage
            value = initial;
            for (int i = 0; i < keys; i++) {
                assertArrayEquals(value.toByteArray(), dao.get(keyFrom(i)));
                value = next(value);
            }
        } finally {
            dao.close();
            Files.recursiveDelete(data);
        }
    }

    @Ignore("Just do it! (if you can)")
    @Test
    public void bulkReplace() throws IOException {
        final int keys = 10_000;
        final int ops = 10 * keys;
        final BigInteger initial = initial();

        final File data = Files.createTempDirectory();
        KVDao dao = KVDaoFactory.create(data);
        try {
            // Fill the storage
            BigInteger value = initial;
            for (int i = 0; i < ops; i++) {
                dao.upsert(keyFrom(i % keys), value.toByteArray());
                value = next(value);
            }

            // Reopen
            dao.close();
            dao = KVDaoFactory.create(data);

            // Check the storage

            // Iterate up to the last cycle
            value = initial;
            for (int i = 0; i < ops - keys; i++) {
                value = next(value);
            }

            // Check values
            for (int i = ops - keys; i < ops; i++) {
                assertArrayEquals(value.toByteArray(), dao.get(keyFrom(i % keys)));
                value = next(value);
            }
        } finally {
            dao.close();
            Files.recursiveDelete(data);
        }
    }
}
