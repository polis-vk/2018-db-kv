package ru.mail.polis.alexeykotelevskiy;


import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

import org.jetbrains.annotations.NotNull;



public class BTreeNode<K extends Comparable<? super K> , V> implements Comparable<BTreeNode>, Externalizable {





    public static final int HALF_MAX = 20;


    private java.util.ArrayList<K> data;
    public java.util.ArrayList<V> values;


    private java.util.ArrayList<Integer> children;


    private int id;
    public BTreeNode(){}

    public BTreeNode(boolean leaf) {

        this.id = IdGenerator.nextId();
        data = new java.util.ArrayList<K>((HALF_MAX * 2) - 1);
        values = new java.util.ArrayList<V>((HALF_MAX * 2) - 1);
        if (!leaf) {
            children = new java.util.ArrayList<Integer>(HALF_MAX * 2);
        }
    }


    public BTreeNode(BTreeNode<K,V> child) {
        this(false);
        children.add(child.getId());
        splitChild(0, child);
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int compareTo(@NotNull BTreeNode o) {
        if (this.getId() < o.getId())
        {
            return -1;
        }
        if (this.getId() > o.getId())
        {
            return 1;
        }
        return 0;
    }




    public void add(K key, V value) {
        BTreeNode<K, V> node = this;
        while (!(node.isLeaf())) {
            int loc = node.indexOf(key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                return;
            } else {
                BTreeNode child = node.getChild(childIndex);
                if (child.isFull()) {
                    node.splitChild(childIndex, child);
                } else {
                    node.writeToDisk();
                    node = child;
                }
            }
        }
        node.addLocally(key, value);
        node.writeToDisk();
    }


    protected void addLocally(K key, V value) {
        int loc = indexOf(key);
        int i = loc >= 0 ? loc + 1 : -loc - 1;
        if (loc < 0) {
            data.add(i, key);
            values.add(i, value);
            if (!isLeaf()) {
                children.add(i + 1, 0);
            }
        } else
        {
            data.set(loc, key);
            values.set(loc, value);
        }
    }


    protected BTreeNode createRightSibling() {
        BTreeNode<K, V> sibling = new BTreeNode<K, V>(isLeaf());
        for (int i = HALF_MAX; i < (HALF_MAX * 2) - 1; i++) {
            sibling.data.add(data.remove(HALF_MAX));
            sibling.values.add(values.remove(HALF_MAX));
        }
        if (!isLeaf()) {
            for (int i = HALF_MAX; i < HALF_MAX * 2; i++) {
                sibling.children.add(children.remove(HALF_MAX));
            }
        }
        sibling.writeToDisk();
        return sibling;
    }


    public void deleteFromDisk() {
        try {
            File file = new File(BTree.DIR + "b" + id + ".node");
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    public BTreeNode<K, V> getChild(int index) {
        if (isLeaf()) {
            return null;
        } else {
            return readFromDisk(children.get(index));
        }
    }


    public int getId() {
        return id;
    }


    public int indexOf(K key) {
        int loc = Collections.binarySearch(data, key);
        return loc;
    }


    public boolean isFull() {
        return size() == HALF_MAX * 2;
    }


    public boolean isLeaf() {
        return children == null;
    }


    public boolean isMinimal() {
        return size() == HALF_MAX;
    }


    protected void mergeChildren(int i, BTreeNode<K, V> child, BTreeNode<K, V> sibling) {
        child.data.add(data.remove(i));
        child.values.add(values.remove(i));
        children.remove(i + 1);
        if (!(child.isLeaf())) {
            child.children.add(sibling.children.remove(0));
        }
        for (int j = 0; j < HALF_MAX - 1; j++) {
            child.data.add(sibling.data.remove(0));
            child.values.add(sibling.values.remove(0));
            if (!(child.isLeaf())) {
                child.children.add(sibling.children.remove(0));
            }
        }
        sibling.deleteFromDisk();
    }


    public static<K extends Comparable<? super K>,V> BTreeNode<K,V> readFromDisk(int id) {
        try {
            ObjectInputStream in
                    = new ObjectInputStream
                    (new FileInputStream(BTree.DIR + "b" + id + ".node"));
            BTreeNode<K,V> bTreeNode = (BTreeNode<K,V>)(in.readObject());
            in.close();
            return bTreeNode;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }


    public void remove(K key) {
        int loc = indexOf(key);
        int i = loc;
        if (isLeaf()) {
            if (loc >= 0) {
                data.remove(i);
                values.remove(i);
                writeToDisk();
            }
        } else if (loc >= 0) {
            removeFromInternalNode(i, key);
        } else {
            removeFromChild(i, key);
        }
    }


    protected void removeFromChild(int i, K key) {
        BTreeNode<K, V> child = getChild(i);
        if (child.isMinimal()) {
            if (i == 0) {             // Target in first child
                BTreeNode<K, V> sibling = getChild(1);
                if (sibling.isMinimal()) {
                    mergeChildren(i, child, sibling);
                } else {
                    rotateLeft(i, child, sibling);
                }
            } else if (i == size() - 1) { // Target in last child
                BTreeNode<K, V> sibling = getChild(i - 1);
                if (sibling.isMinimal()) {
                    mergeChildren(i - 1, sibling, child);
                    child = sibling;
                } else {
                    rotateRight(i - 1, sibling, child);
                }
            } else {                  // Target in middle child
                BTreeNode<K, V> rightSibling = getChild(i + 1);
                BTreeNode<K, V> leftSibling = getChild(i - 1);
                if (!(rightSibling.isMinimal())) {
                    rotateLeft(i, child, rightSibling);
                } else if (!(leftSibling.isMinimal())) {
                    rotateRight(i - 1, leftSibling, child);
                } else {
                    mergeChildren(i, child, rightSibling);
                }
            }
        }
        writeToDisk();
        child.remove(key);
    }


    protected void removeFromInternalNode(int i, K key) {
        BTreeNode<K, V> child = getChild(i);
        BTreeNode<K, V> sibling = getChild(i + 1);
        if (!(child.isMinimal())) {
            Pair<K,V> pair = child.removeRightmost();
            data.set(i, pair.getKey());
            values.set(i, pair.getValue());
            writeToDisk();
        } else if (!(sibling.isMinimal())) {
            Pair<K,V> pair = sibling.removeLeftmost();
            data.set(i, pair.getKey());
            values.set(i, pair.getValue());
            writeToDisk();
        } else {
            mergeChildren(i, child, sibling);
            writeToDisk();
            child.remove(key);
        }
    }


    protected Pair<K, V> removeLeftmost() {
        BTreeNode<K, V> node = this;
        while (!(node.isLeaf())) {
            BTreeNode<K, V> child = node.getChild(0);
            if (child.isMinimal()) {
                BTreeNode<K, V> sibling = node.getChild(1);
                if (sibling.isMinimal()) {
                    node.mergeChildren(0, child, sibling);
                } else {
                    node.rotateLeft(0, child, sibling);
                }
            }
            node.writeToDisk();
            return child.removeLeftmost();
        }
        Pair<K, V> pair = new Pair<>(node.data.get(0), node.values.get(0));
        node.data.remove(0);
        node.values.remove(0);
        node.writeToDisk();
        return pair;
    }


    protected Pair<K, V> removeRightmost() {
        BTreeNode<K, V> node = this;
        while (!(node.isLeaf())) {
            BTreeNode<K, V> child = node.getChild(size() - 1);
            if (child.isMinimal()) {
                BTreeNode<K, V> sibling = node.getChild(size() - 2);
                if (sibling.isMinimal()) {
                    node.mergeChildren(size() - 2, sibling, child);
                    child = sibling;
                } else {
                    node.rotateRight(size() - 2, sibling, child);
                }
            }
            node.writeToDisk();
            return child.removeRightmost();
        }
        Pair<K, V> pair = new Pair<>(node.data.get(0), node.values.get(0));
        node.data.remove(size() - 2);
        node.values.remove(size() - 2);
        node.writeToDisk();
        return pair;
    }


    protected void rotateLeft(int i, BTreeNode<K, V> child,
            BTreeNode<K, V> sibling) {
        child.data.add(data.get(i));
        child.values.add(values.get(i));
        if (!(child.isLeaf())) {
            child.children.add(sibling.children.remove(0));
        }
        data.set(i, sibling.data.remove(0));
        values.set(i, sibling.values.remove(0));
        sibling.writeToDisk();
    }


    protected void rotateRight(int i, BTreeNode<K, V> sibling,
            BTreeNode<K,V> child) {
        child.data.add(0, data.get(i));
        child.values.add(0, values.get(i));
        if (!(child.isLeaf())) {
            child.children.add(0,
                    sibling.children.remove(sibling.size() - 1));
        }
        data.set(i, sibling.data.remove(sibling.size() - 2));
        values.set(i, sibling.values.remove(sibling.size() - 2));
        sibling.writeToDisk();
    }


    public void setLeaf(boolean value) {
        if (value) {
            children = null;
        } else {
            children = new java.util.ArrayList<Integer>(HALF_MAX * 2);
        }
    }


    public int size() {
        return data.size() + 1;
    }


    protected void splitChild(int i, BTreeNode<K, V> child) {
        BTreeNode sibling = child.createRightSibling();
        addLocally(child.data.remove(HALF_MAX - 1), child.values.remove(HALF_MAX - 1));
        child.writeToDisk();
        children.set(i + 1, sibling.getId());
    }


    public void writeToDisk() {

        try {
            ObjectOutputStream out
                    = new ObjectOutputStream
                    (new FileOutputStream(BTree.DIR + "b" + id + ".node"));
            out.writeObject((BTreeNode<K,V>)this);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
        out.writeObject(values);
        out.writeObject(children);
        out.writeInt(id);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (ArrayList<K>) in.readObject();
        values = (ArrayList<V>) in.readObject();
        children = (ArrayList<Integer>) in.readObject();
        id = in.readInt();
    }
}