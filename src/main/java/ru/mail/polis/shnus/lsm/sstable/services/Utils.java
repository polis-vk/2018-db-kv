package ru.mail.polis.shnus.lsm.sstable.services;

import java.io.File;
import java.nio.ByteBuffer;

public class Utils {
    public final static String DATA_FOLDER = "data";
    public final static String INDEX_FOLDER = "index";
    public final static String SEPARATOR = "_";
    public final static String INDEX_MARKER = "index";

    public final static int LONG_TO_BYTE_LENGTH = 8;
    public final static int BOOLEAN_TO_BYTE_LENGTH = 1;
    public final static int SSTABLE_FILE_SIZE = 60_000_000;
    public final static int BUFFER_SIZE = 1_048_576;

    //thx to https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }


    public static String getPath(File directory, String fileName) {
        return directory + File.separator + fileName;
    }

    public static String getDataNameByNumber(int n) {
        return DATA_FOLDER + SEPARATOR + n;
    }

    public static String getIndexNameByNumber(int n) {
        return DATA_FOLDER + SEPARATOR + n + SEPARATOR + INDEX_MARKER;
    }

    public static long getNumberFromIndexPath(String path) {
        String[] s = path.split(SEPARATOR);
        return Integer.valueOf(s[s.length - 2]);
    }
}
