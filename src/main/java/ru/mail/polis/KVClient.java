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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;

/**
 * Simple console client to {@link KVDao}
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
public class KVClient {
    private static final String DATA = "data";

    public static void main(String[] args) throws IOException {
        final File data = new File(DATA);
        if (!data.exists()) {
            if (!data.mkdir()) {
                throw new IOException("Can't create directory: " + data);
            }
        }
        if (!data.isDirectory()) {
            throw new IOException("Not directory: " + data);
        }

        final KVDao dao = KVDaoFactory.create(data);
        final String pkg = dao.getClass().getPackage().toString();
        System.out.println(
                "Welcome to " + pkg.substring(pkg.lastIndexOf(".") + 1) + " Key-Value DAO!"
                        + "\nStoring data in directory " + DATA
                        + "\nSupported commands:"
                        + "\n\tget <key>"
                        + "\n\tput <key> <value>"
                        + "\n\tremove <key>"
                        + "\n\tquit");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while (!"quit".equals(line = reader.readLine())) {
                if (line.isEmpty()) {
                    continue;
                }

                final String[] tokens = line.split(" ");
                final String cmd = tokens[0];
                final String key = tokens[1];

                switch (cmd) {
                    case "get":
                        try {
                            System.out.println(new String(dao.get(key.getBytes())));
                        } catch (NoSuchElementException e) {
                            System.err.println("absent");
                        }
                        break;

                    case "put":
                        final String value = tokens[2];
                        dao.upsert(key.getBytes(), value.getBytes());
                        break;

                    case "remove":
                        dao.remove(key.getBytes());
                        break;

                    default:
                        System.err.println("Unsupported command: " + cmd);
                }
            }
        }
    }
}
