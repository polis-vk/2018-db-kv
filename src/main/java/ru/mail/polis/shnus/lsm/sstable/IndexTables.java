package ru.mail.polis.shnus.lsm.sstable;

import ru.mail.polis.shnus.ByteWrapper;
import ru.mail.polis.shnus.lsm.sstable.model.KeyAndOffset;
import ru.mail.polis.shnus.lsm.sstable.model.SSTableLocation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexTables implements Closeable {
    private final File indexPath;
    private final File dataPath;
    List<Index> indexes;

    public IndexTables(File dataPath, File indexPath) throws IOException {
        this.indexPath = indexPath;
        this.dataPath = dataPath;
        Index.indexPath = indexPath;
        Index.dataPath = dataPath;
        indexes = new ArrayList<>();
        loadIndexes();
    }

    private void loadIndexes() throws IOException {
        List<Integer> numbers = getNumbers();
        Index nextIndex;
        for (Integer number : numbers) {
            nextIndex = new Index(number);
            if (!nextIndex.isEmpty()) {
                indexes.add(nextIndex);
            } else {
                nextIndex.close();
            }
        }
    }

    private List<Integer> getNumbers() {
        File[] files = indexPath.listFiles();
        List<Integer> numbers = new ArrayList<>();

        //TODO by stream api
        for (File file : files) { //no need to check that file is file and not directory
            String fileName = file.getName();
            //TODO need regex
            if (fileName.compareTo("index") != 0) {
                int currentFileNumber = Integer.valueOf(fileName.split("_")[1]);
                numbers.add(currentFileNumber);
            }
        }

        return numbers;
    }

    public SSTableLocation getValueLocationByKey(byte[] key) {
        ByteWrapper keyWrapper = new ByteWrapper(key);
        SSTableLocation location = null;
        SSTableLocation curLocation;
        long maxTimeStamp = 0;
        Index index;
        for (int i = indexes.size() - 1; i >= 0; i--) {
            index = indexes.get(i);

            if (index.getTimeStamp() > maxTimeStamp) {
                curLocation = index.findAndGetKeyLocation(keyWrapper);
                if (curLocation != null) {
                    location = curLocation;
                    maxTimeStamp = index.getTimeStamp();
                }
            }
        }

        return location;
    }

    public void removeValueByKey(ByteWrapper keyWrapper) throws IOException {
        Index index;
        int position;

        for (int i = 0; i < indexes.size(); i++) {
            index = indexes.get(i);

            position = index.findAndGetRemoveMarkerLocation(keyWrapper);
            if (position != -1) {
                index.markAsRemoved(position);
                index.removeFromMemory(keyWrapper);
            }
        }
    }


    public void addIndexByList(List<KeyAndOffset> index, long fileNumber, long timeStamp) throws IOException {
        indexes.add(new Index(index, fileNumber, timeStamp));
    }

    @Override
    public void close() throws IOException {
        for (Index index : indexes) {
            index.close();
        }
    }

}
