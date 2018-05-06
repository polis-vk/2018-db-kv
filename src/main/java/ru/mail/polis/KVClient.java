package ru.mail.polis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;

public class KVClient {
    public static void main(String[] args) throws IOException {
        final KVDao dao = KVDaoFactory.create();
        final String pkg = dao.getClass().getPackage().toString();
        System.out.println(
                "Welcome to " + pkg.substring(pkg.lastIndexOf(".") + 1) + " Key-Value DAO!"
                        + "\nSupported commands:"
                        + "\n\tget <key>"
                        + "\n\tput <key> <value>"
                        + "\n\tremove <key>"
                        + "\n\tquit");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            try {
                while (true) {

                    line = reader.readLine();

                    if ("quit".equals(line)) break;


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
                            try {
                                final String value = tokens[2];
                                dao.upsert(key.getBytes(), value.getBytes());
                            } catch (ArrayIndexOutOfBoundsException e) {
                                System.err.println("invalid command, please use 'put <key> <value>'");
                            }
                            break;

                        case "remove":
                            dao.remove(key.getBytes());
                            break;

                        default:
                            System.err.println("Unsupported command: " + cmd);
                    }
                }
            } finally {
                dao.close();
            }
        }
    }
}
