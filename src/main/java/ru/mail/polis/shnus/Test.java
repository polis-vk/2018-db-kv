package ru.mail.polis.shnus;

import ru.mail.polis.shnus.lsm.sstable.services.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class Test {
    public static void main(String[] args) throws IOException {
        String path = "data"+File.separator+"data_1";
        RandomAccessFile ras = new RandomAccessFile(path, "rwd");
        System.out.println(ras.length());
        byte[] bytes = new byte[2];
        ras.seek(6);
        ras.read(bytes, 0, 2);
        System.out.println(new String(bytes));

    }
}