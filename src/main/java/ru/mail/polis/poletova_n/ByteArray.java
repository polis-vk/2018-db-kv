package ru.mail.polis.poletova_n;

import java.util.Arrays;

public class ByteArray {

    private final byte[] mas;



    public ByteArray(byte[] mas){
        this.mas=mas;
    }

    private byte[] getMas(){
        return mas;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(mas);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ByteArray) {
            return Arrays.equals(mas, ((ByteArray) obj).getMas() );
        }
        return false;
    }
}
