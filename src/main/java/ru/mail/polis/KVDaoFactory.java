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

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.kirillyarulin.DiskKVDao;
import ru.mail.polis.kirillyarulin.DiskKVDao2;

import java.io.File;
import java.io.IOException;

/**
 * Custom {@link KVDao} factory
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public final class KVDaoFactory {
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    private KVDaoFactory() {
        // Not instantiatable
    }

    /**
     * Construct a {@link KVDao} instance.
     *
     * @param data local disk folder to persist the data to
     * @return a storage instance
     */
    @NotNull
    public static KVDao create(@NotNull final File data) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (!data.exists()) {
            throw new IllegalArgumentException("Path doesn't exist: " + data);
        }

        if (!data.isDirectory()) {
            throw new IllegalArgumentException("Path is not a directory: " + data);
        }

        return new DiskKVDao2(data);


    }
}
