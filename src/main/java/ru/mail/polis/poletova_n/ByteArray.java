package ru.mail.polis.poletova_n;

public class ByteArray {

    private byte[] mas;

    public byte[] getMas() {
        return mas;
    }

    public ByteArray(byte[] mas){
        this.mas=mas;
    }

    public int length(){
        return mas.length;
    }

    @Override
    public int hashCode() {
        int z=0;
        for (int i = 0;i<mas.length;i++){
            z+=(i+1)*mas[i];
        }
        return z;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null){
            return false;
        }
        if(obj instanceof ByteArray){
            if (((ByteArray) obj).length()== mas.length){
                byte[] m =((ByteArray) obj).getMas();
                for (int i = 0; i < mas.length; i++) {
                    if(mas[i]!=m[i]){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }
}
