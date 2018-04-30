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

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Persistence tests for {@link KVDao} implementations
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public class PersistenceTest extends TestBase {
    @Test(expected = NoSuchElementException.class)
    public void fs() throws IOException {
        // Reference key
        final byte[] key = randomKey();

        // Create, fill and remove storage
        final File data = Files.createTempDirectory();
        try {
            final KVDao dao = KVDaoFactory.create(data);
            dao.upsert(key, randomValue());
            dao.close();
        } finally {
            Files.recursiveDelete(data);
        }

        // Check that the storage is empty
        assertFalse(data.exists());
        assertTrue(data.mkdir());
        try {
            final KVDao dao = KVDaoFactory.create(data);
            dao.get(key);
            fail();
        } finally {
            Files.recursiveDelete(data);
        }
    }

    @Test
    public void reopen() throws IOException {
        // Reference value
        final byte[] key = randomKey();
        final byte[] value = randomValue();

        final File data = Files.createTempDirectory();
        try {
            // Create, fill and close storage
            KVDao dao = KVDaoFactory.create(data);
            dao.upsert(key, value);
            dao.close();

            // Recreate dao
            dao = KVDaoFactory.create(data);
            assertArrayEquals(value, dao.get(key));
        } finally {
            Files.recursiveDelete(data);
        }
    }
}
