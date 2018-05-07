package ru.mail.polis.shnus.lsm.sstable.services;

import java.io.File;
import java.nio.ByteBuffer;

public class Utils {
    public static final String SEPARATOR = "_";
    public static final String INDEX_MARKER = "index";


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
        return "data" + SEPARATOR + n;
    }

    public static String getIndexNameByNumber(int n) {
        return "data" + SEPARATOR + n + SEPARATOR + INDEX_MARKER;
    }

    public static long getNumberFromIndexPath(String path) {
        String[] s = path.split("_");
        return Integer.valueOf(s[s.length - 2]);
    }
}
