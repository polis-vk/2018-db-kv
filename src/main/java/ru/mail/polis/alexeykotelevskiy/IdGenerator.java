package ru.mail.polis.alexeykotelevskiy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Generates unique id numbers, even across multiple sessions.
 */
public class IdGenerator {

    /**
     * File in which the next available id is stored.
     */
    public static File FILE = new File(BTree.DIR + "id");

    /**
     * Return the next available id number.
     */
    public static int nextId() {
        try {
            int result;
            if (FILE.exists()) {
                ObjectInputStream in
                        = new ObjectInputStream(new FileInputStream(FILE));
                result = in.readInt();
            } else {
                result = 0;
            }
            ObjectOutputStream out
                    = new ObjectOutputStream(new FileOutputStream(FILE));
            out.writeInt(result + 1);
            out.close();
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
            return 0;
        }
    }

    public static void main(String[] args) {
        System.out.println(nextId());
    }
}