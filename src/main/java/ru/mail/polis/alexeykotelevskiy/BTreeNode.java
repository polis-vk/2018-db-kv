package ru.mail.polis.alexeykotelevskiy;

// Introduced in Chapter 17
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

import org.jetbrains.annotations.NotNull;

import javafx.util.Pair;

/** Node in a BTree. */
public class BTreeNode<K extends Comparable<? super K> , V> implements Comparable<BTreeNode>, Externalizable {

    static CashTable cashTable;



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




    /** Minimum number of children.  Max is twice this. */
    public static final int HALF_MAX = 20;

    /** Items stored in this node. */
    private java.util.ArrayList<K> data;
    public java.util.ArrayList<V> values;

    /** Ids of children of this node. */
    private java.util.ArrayList<Integer> children;

    /** Number identifying this node. */
    private int id;

    /**
     * The new node has no data or children yet.  The argument
     * leaf specifies whether it is a leaf.
     */
    public BTreeNode(boolean leaf) {

        this.id = IdGenerator.nextId();
        data = new java.util.ArrayList<K>((HALF_MAX * 2) - 1);
        values = new java.util.ArrayList<V>((HALF_MAX * 2) - 1);
        if (!leaf) {
            children = new java.util.ArrayList<Integer>(HALF_MAX * 2);
        }
    }

    /**
     * Create a new node that has two children, each containing
     * half of the items from child.  Write the children to disk.
     */
    public BTreeNode(BTreeNode<K,V> child) {
        this(false);
        children.add(child.getId());
        splitChild(0, child);
    }

    /**
     * Add target to the subtree rooted at this node.  Write nodes
     * to disk as necessary.
     */
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

    /**
     * Add target to this node, which is assumed to not be full.
     * Make room for an extra child to the right of target.
     */
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

    /**
     * Create and return a new node which will be a right sibling
     * of this one.  Half of the items and children in this node are
     * copied to the new one.
     */
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

    /** Delete the file containing this node from the disk. */
    public void deleteFromDisk() {
        try {
            File file = new File(BTree.DIR + "b" + id + ".node");
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Read the ith child of this node from the disk and return it.
     * If this node is a leaf, return null.
     */
    public BTreeNode<K, V> getChild(int index) {
        if (isLeaf()) {
            return null;
        } else {
            return readFromDisk(children.get(index));
        }
    }

    /** Return the id of this node. */
    public int getId() {
        return id;
    }

    /**
     * Return the index of target in this node if present.  Otherwise,
     * return the index of the child that would contain target,
     * plus 0.5.
     */
    public int indexOf(K key) {
        int loc = Collections.binarySearch(data, key);
        return loc;
    }

    /** Return true if this node is full. */
    public boolean isFull() {
        return size() == HALF_MAX * 2;
    }

    /** Return true if this node is a leaf. */
    public boolean isLeaf() {
        return children == null;
    }

    /** Return true if this node is minimal. */
    public boolean isMinimal() {
        return size() == HALF_MAX;
    }

    /**
     * Merge this node's ith and (i+1)th children (child and sibling,
     * both minimal), moving the ith item down from this node.
     * Delete sibling from disk.
     */
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

    /** Read from disk and return the node with the specified id. */
    public static<K extends Comparable<? super K>,V> BTreeNode<K,V> readFromDisk(int id) {

        BTreeNode<K,V> node = cashTable.getNode(id);

        if (node!=null){
            return node;
        }

        try {
            ObjectInputStream in
                    = new ObjectInputStream
                    (new FileInputStream(BTree.DIR + "b" + id + ".node"));
            BTreeNode<K,V> bTreeNode = (BTreeNode<K,V>)(in.readObject());
            return bTreeNode;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * Remove target from the subtree rooted at this node.
     * Write any modified nodes to disk.
     */
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

    /**
     * Remove target from the subtree rooted at child i of this node.
     * Write any modified nodes to disk.
     */
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

    /**
     * Remove the ith item (target) from this node.
     * Write any modified nodes to disk.
     */
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

    /**
     * Remove and return the leftmost element in the leftmost descendant
     * of this node.  Write any modified nodes to disk.
     */
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

    /**
     * Remove and return the rightmost element in the rightmost
     * descendant of this node.  Write any modified nodes to disk.
     */
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

    /**
     * Child is the ith child of this node, sibling the (i+1)th.
     * Move one item from sibling up into this node, one from this
     * node down into child.  Pass one child from sibling to node.
     * Write sibling to disk.
     */
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

    /**
     * Sibling is the ith child of this node, child the (i+1)th.
     * Move one item from sibling up into this node, one from this
     * node down into child.  Pass one child from sibling to node.
     * Write sibling to disk.
     */
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

    /** Make this node a leaf if value is true, not a leaf otherwise. */
    public void setLeaf(boolean value) {
        if (value) {
            children = null;
        } else {
            children = new java.util.ArrayList<Integer>(HALF_MAX * 2);
        }
    }

    /** Return one plus the number of items in this node. */
    public int size() {
        return data.size() + 1;
    }

    /**
     * Split child, which is the full ith child of this node, into
     * two minimal nodes, moving the middle item up into this node.
     */
    protected void splitChild(int i, BTreeNode<K, V> child) {
        BTreeNode sibling = child.createRightSibling();
        addLocally(child.data.remove(HALF_MAX - 1), child.values.remove(HALF_MAX - 1));
        child.writeToDisk();
        children.set(i + 1, sibling.getId());
    }

    /** Write this node to disk. */
    public void writeToDisk() {
        cashTable.setNode(this);
        try {
            ObjectOutputStream out
                    = new ObjectOutputStream
                    (new FileOutputStream(BTree.DIR + "b" + id + ".node"));
            out.writeObject(this);
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
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        System.out.println("OK");
        data = (ArrayList<K>)in.readObject();
        values = (ArrayList<V>)in.readObject();
        children = (ArrayList<Integer>)in.readObject();
    }
}