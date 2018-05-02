package ru.mail.polis.alexeykotelevskiy;


import java.io.*;


public class BTree<K extends Comparable<? super K>, V> implements Serializable {


    public static String DIR
            = BTree.class.getProtectionDomain().getCodeSource()
            .getLocation().getFile() + File.separator;


    private int rootId;

    private void createStruct() {
        BTreeNode<K, V> root = new BTreeNode<K, V>(true);

        rootId = root.getId();
        root.writeToDisk();
        writeToDisk();
    }

    public BTree() {
        createStruct();
    }

    public BTree(String path) {
        DIR = path + File.separator;
        IdGenerator.FILE = new File(BTree.DIR + "id");
        createStruct();
    }


    public void add(K key, V value) {
        BTreeNode<K, V> root = BTreeNode.readFromDisk(rootId);
        if (root.isFull()) {
            BTreeNode<K, V> parent = new BTreeNode<K, V>(root);
            rootId = parent.getId();
            writeToDisk();
            parent.add(key, value);
        } else {
            root.add(key, value);
        }
    }


    public V search(K target) {
        BTreeNode<K, V> node = BTreeNode.readFromDisk(rootId);
        while (node != null) {
            int loc = node.indexOf(target);
            int i = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                return node.values.get(loc);
            } else {
                node = node.getChild(i);
            }
        }
        return null;
    }


    public boolean contains(K target) {
        BTreeNode<K, V> node = BTreeNode.readFromDisk(rootId);
        while (node != null) {
            double d = node.indexOf(target);
            int i = (int) d;
            if (i == d) {
                return true;
            } else {
                node = node.getChild(i);
            }
        }
        return false;
    }

    public static <K extends Comparable<? super K>, V> BTree<K, V> readFromDisk(String path) {
        try {
            ObjectInputStream in
                    = new ObjectInputStream
                    (new FileInputStream(path));
            BTree<K, V> a = (BTree<K, V>) (in.readObject());
            in.close();

            return a;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }


    public void remove(K key) {
        BTreeNode<K, V> root = BTreeNode.readFromDisk(rootId);
        root.remove(key);
        if ((root.size() == 1) && (!(root.isLeaf()))) {
            BTreeNode<K, V> child = root.getChild(0);
            root.deleteFromDisk();
            rootId = child.getId();
            writeToDisk();
        }
    }


    public void writeToDisk() {

        try {
            ObjectOutputStream out
                    = new ObjectOutputStream
                    (new FileOutputStream(DIR + "btree"));
            out.writeObject(this);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        BTree<Integer, Integer> bTree = new BTree<>();
        bTree.add(1, 2);
        bTree.add(2, 2);
        bTree.add(3, 2);
        bTree.add(4, 2);
        bTree.add(5, 2);
        bTree.add(5, 3);
        System.out.println(bTree.search(5));
    }
}