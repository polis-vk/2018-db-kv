package ru.mail.polis.alexeykotelevskiy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;


class CashTable<K extends Comparable<? super K>,V> implements Serializable{
    Random random = new Random();

    java.util.List<BTreeNode<K,V> > cashTable = new ArrayList<>();
    int maxSize;
    CashTable<K,V> instance = null;


    CashTable(){
        long maxMem = Runtime.getRuntime().maxMemory();
        maxSize = (int) maxMem / 1024 / 100;
    }

    void setNode(BTreeNode<K, V> node)
    {
        int id = Collections.binarySearch(cashTable, node);

        if (id >=0) return;
        cashTable.add(-id-1, node);
        if (cashTable.size() > maxSize)
        {
         cashTable.remove(random.nextInt()%maxSize);
        }
    }


    BTreeNode<K,V> getNode(int id)
    {

        BTreeNode<K,V> bTreeNode = new BTreeNode<K, V>(false);
        bTreeNode.setId(id);
        int index = Collections.binarySearch(cashTable, bTreeNode);
        return index>=0 ? cashTable.get(index) : null;
    }


}
