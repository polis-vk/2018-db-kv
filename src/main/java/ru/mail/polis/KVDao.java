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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Key-value DAO API
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public interface KVDao extends Closeable {
    @NotNull
    byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException;

    void upsert(
            @NotNull byte[] key,
            @NotNull byte[] value) throws IOException;

    void remove(@NotNull byte[] key) throws IOException;
}
