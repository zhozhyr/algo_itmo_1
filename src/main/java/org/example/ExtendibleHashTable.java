package org.example;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ExtendibleHashTable implements AutoCloseable {

    private static final int MAX_GLOBAL_DEPTH = 20;

    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    private static final int DIRECTORY_HEADER_SIZE = 8; // globalDepth + nextBucketId
    private static final int BUCKET_HEADER_SIZE = 8;    // localDepth + size
    private static final int RECORD_SIZE = 1 + LONG_SIZE + LONG_SIZE; // used + key + value

    private static final int DIR_GLOBAL_DEPTH_OFFSET = 0;
    private static final int DIR_NEXT_BUCKET_ID_OFFSET = 4;

    private static final int BUCKET_LOCAL_DEPTH_OFFSET = 0;
    private static final int BUCKET_SIZE_OFFSET = 4;

    private final Path rootDir;
    private final Path bucketsDir;
    private final Path directoryFile;
    private final int bucketCapacity;
    private final int bucketFileSize;
    private final int directoryFileSize;
    private final Map<Integer, FileHandle> bucketHandles = new HashMap<>();
    private final FileHandle directoryHandle;

    public ExtendibleHashTable(Path rootDir, int bucketCapacity) throws IOException {
        this.rootDir = rootDir;
        this.bucketsDir = rootDir.resolve("buckets");
        this.directoryFile = rootDir.resolve("directory.dat");
        this.bucketCapacity = bucketCapacity;

        this.bucketFileSize = BUCKET_HEADER_SIZE + bucketCapacity * RECORD_SIZE;
        this.directoryFileSize = DIRECTORY_HEADER_SIZE + (1 << MAX_GLOBAL_DEPTH) * INT_SIZE;

        initStorage();
        this.directoryHandle = openFileHandle(directoryFile, directoryFileSize);
    }

    public void put(long key, long value) throws IOException {
        while (true) {
            int globalDepth = getGlobalDepth();
            int dirIndex = directoryIndex(key, globalDepth);
            int bucketId = getDirectoryEntry(dirIndex);

            Bucket bucket = readBucketHeader(bucketId);

            int pos = findKeyInBucket(bucketId, key, bucket.size);
            if (pos != -1) {
                writeRecordValue(bucketId, pos, value);
                return;
            }

            if (bucket.size < bucketCapacity) {
                writeRecord(bucketId, bucket.size, (byte) 1, key, value);
                setBucketSize(bucketId, bucket.size + 1);
                return;
            }

            splitBucket(bucketId, bucket.localDepth);
        }
    }

    public Long get(long key) throws IOException {
        int globalDepth = getGlobalDepth();
        int dirIndex = directoryIndex(key, globalDepth);
        int bucketId = getDirectoryEntry(dirIndex);

        Bucket bucket = readBucketHeader(bucketId);
        int pos = findKeyInBucket(bucketId, key, bucket.size);
        if (pos == -1) {
            return null;
        }

        return readRecord(bucketId, pos).value;
    }

    public boolean update(long key, long value) throws IOException {
        int globalDepth = getGlobalDepth();
        int dirIndex = directoryIndex(key, globalDepth);
        int bucketId = getDirectoryEntry(dirIndex);

        Bucket bucket = readBucketHeader(bucketId);
        int pos = findKeyInBucket(bucketId, key, bucket.size);
        if (pos == -1) {
            return false;
        }

        writeRecordValue(bucketId, pos, value);
        return true;
    }

    public boolean remove(long key) throws IOException {
        int globalDepth = getGlobalDepth();
        int dirIndex = directoryIndex(key, globalDepth);
        int bucketId = getDirectoryEntry(dirIndex);

        Bucket bucket = readBucketHeader(bucketId);
        int pos = findKeyInBucket(bucketId, key, bucket.size);
        if (pos == -1) {
            return false;
        }

        int lastIndex = bucket.size - 1;
        if (pos != lastIndex) {
            Record last = readRecord(bucketId, lastIndex);
            writeRecord(bucketId, pos, last.used, last.key, last.value);
        }

        clearRecord(bucketId, lastIndex);
        setBucketSize(bucketId, lastIndex);
        return true;
    }

    public void printState() throws IOException {
        int globalDepth = getGlobalDepth();
        int nextBucketId = getNextBucketId();

        System.out.println("globalDepth = " + globalDepth);
        System.out.println("nextBucketId = " + nextBucketId);

        System.out.println("Directory:");
        for (int i = 0; i < (1 << globalDepth); i++) {
            System.out.println(i + " -> bucket_" + getDirectoryEntry(i));
        }

        System.out.println("Buckets:");
        for (int bucketId = 0; bucketId < nextBucketId; bucketId++) {
            Path path = bucketPath(bucketId);
            if (!Files.exists(path)) continue;

            Bucket bucket = readBucketHeader(bucketId);
            System.out.print("bucket_" + bucketId +
                    " [localDepth=" + bucket.localDepth + ", size=" + bucket.size + "]: ");

            for (int i = 0; i < bucket.size; i++) {
                Record r = readRecord(bucketId, i);
                if (r.used == 1) {
                    System.out.print("(" + r.key + " -> " + r.value + ") ");
                }
            }
            System.out.println();
        }
    }

    private void initStorage() throws IOException {
        Files.createDirectories(rootDir);
        Files.createDirectories(bucketsDir);

        if (Files.exists(directoryFile)) {
            return;
        }

        try (FileHandle handle = openFileHandle(directoryFile, directoryFileSize)) {
            MappedByteBuffer buffer = handle.buffer;
            int initialGlobalDepth = 1;
            int initialNextBucketId = 1;

            buffer.putInt(DIR_GLOBAL_DEPTH_OFFSET, initialGlobalDepth);
            buffer.putInt(DIR_NEXT_BUCKET_ID_OFFSET, initialNextBucketId);

            int dirSize = 1 << initialGlobalDepth;
            for (int i = 0; i < dirSize; i++) {
                buffer.putInt(directoryOffset(i), 0);
            }
        }

        createEmptyBucket(0, 1);
    }

    private void splitBucket(int bucketId, int oldLocalDepth) throws IOException {
        if (oldLocalDepth >= MAX_GLOBAL_DEPTH) {
            throw new IllegalStateException(
                    "Cannot split bucket_" + bucketId + ": localDepth reached MAX_GLOBAL_DEPTH=" + MAX_GLOBAL_DEPTH
            );
        }

        int globalDepth = getGlobalDepth();

        if (oldLocalDepth == globalDepth) {
            doubleDirectory(globalDepth);
            globalDepth = getGlobalDepth();
        }

        int newBucketId = getNextBucketId();
        setNextBucketId(newBucketId + 1);

        createEmptyBucket(newBucketId, oldLocalDepth + 1);
        setBucketLocalDepth(bucketId, oldLocalDepth + 1);

        for (int i = 0; i < (1 << globalDepth); i++) {
            if (getDirectoryEntry(i) == bucketId) {
                int bit = (i >> oldLocalDepth) & 1;
                if (bit == 1) {
                    setDirectoryEntry(i, newBucketId);
                }
            }
        }

        Bucket oldBucket = readBucketHeader(bucketId);
        Record[] records = new Record[oldBucket.size];
        for (int i = 0; i < oldBucket.size; i++) {
            records[i] = readRecord(bucketId, i);
        }

        clearBucket(bucketId);
        clearBucket(newBucketId);

        for (Record r : records) {
            int dirIndex = directoryIndex(r.key, getGlobalDepth());
            int targetBucketId = getDirectoryEntry(dirIndex);

            Bucket target = readBucketHeader(targetBucketId);
            writeRecord(targetBucketId, target.size, (byte) 1, r.key, r.value);
            setBucketSize(targetBucketId, target.size + 1);
        }

        // A split may legitimately leave one side empty when all records share
        // the current prefix; the next split will use a deeper bit.
    }

    private void doubleDirectory(int oldGlobalDepth) throws IOException {
        if (oldGlobalDepth >= MAX_GLOBAL_DEPTH) {
            throw new IllegalStateException(
                    "Cannot double directory: globalDepth reached MAX_GLOBAL_DEPTH=" + MAX_GLOBAL_DEPTH
            );
        }

        int oldSize = 1 << oldGlobalDepth;
        for (int i = 0; i < oldSize; i++) {
            setDirectoryEntry(i + oldSize, getDirectoryEntry(i));
        }
        setGlobalDepth(oldGlobalDepth + 1);
    }

    private int directoryIndex(long key, int globalDepth) {
        return hash(key) & ((1 << globalDepth) - 1);
    }

    private int hash(long key) {
        long h = key ^ (key >>> 32);
        return ((int) h) & 0x7fffffff;
    }

    private void createEmptyBucket(int bucketId, int localDepth) throws IOException {
        withBucket(bucketId, buffer -> {
            buffer.putInt(BUCKET_LOCAL_DEPTH_OFFSET, localDepth);
            buffer.putInt(BUCKET_SIZE_OFFSET, 0);

            for (int i = 0; i < bucketCapacity; i++) {
                clearRecord(buffer, i);
            }
        });
    }

    private void clearBucket(int bucketId) throws IOException {
        withBucket(bucketId, buffer -> {
            int localDepth = buffer.getInt(BUCKET_LOCAL_DEPTH_OFFSET);
            buffer.putInt(BUCKET_LOCAL_DEPTH_OFFSET, localDepth);
            buffer.putInt(BUCKET_SIZE_OFFSET, 0);

            for (int i = 0; i < bucketCapacity; i++) {
                clearRecord(buffer, i);
            }
        });
    }

    private Bucket readBucketHeader(int bucketId) throws IOException {
        return withBucketResult(bucketId, buffer ->
                new Bucket(
                        buffer.getInt(BUCKET_LOCAL_DEPTH_OFFSET),
                        buffer.getInt(BUCKET_SIZE_OFFSET)
                )
        );
    }

    private int findKeyInBucket(int bucketId, long key, int size) throws IOException {
        return withBucketResult(bucketId, buffer -> {
            for (int i = 0; i < size; i++) {
                int offset = recordOffset(i);
                if (buffer.get(offset) == 1 && buffer.getLong(offset + 1) == key) {
                    return i;
                }
            }
            return -1;
        });
    }

    private Record readRecord(int bucketId, int index) throws IOException {
        return withBucketResult(bucketId, buffer -> {
            int offset = recordOffset(index);
            return new Record(
                    buffer.get(offset),
                    buffer.getLong(offset + 1),
                    buffer.getLong(offset + 1 + LONG_SIZE)
            );
        });
    }

    private void writeRecord(int bucketId, int index, byte used, long key, long value) throws IOException {
        withBucket(bucketId, buffer -> {
            int offset = recordOffset(index);
            buffer.put(offset, used);
            buffer.putLong(offset + 1, key);
            buffer.putLong(offset + 1 + LONG_SIZE, value);
        });
    }

    private void writeRecordValue(int bucketId, int index, long value) throws IOException {
        withBucket(bucketId, buffer -> {
            int offset = recordOffset(index);
            buffer.putLong(offset + 1 + LONG_SIZE, value);
        });
    }

    private void clearRecord(int bucketId, int index) throws IOException {
        withBucket(bucketId, buffer -> clearRecord(buffer, index));
    }

    private void clearRecord(MappedByteBuffer buffer, int index) {
        int offset = recordOffset(index);
        buffer.put(offset, (byte) 0);
        buffer.putLong(offset + 1, 0L);
        buffer.putLong(offset + 1 + LONG_SIZE, 0L);
    }

    private void setBucketSize(int bucketId, int size) throws IOException {
        withBucket(bucketId, buffer -> buffer.putInt(BUCKET_SIZE_OFFSET, size));
    }

    private void setBucketLocalDepth(int bucketId, int localDepth) throws IOException {
        withBucket(bucketId, buffer -> buffer.putInt(BUCKET_LOCAL_DEPTH_OFFSET, localDepth));
    }

    private int getGlobalDepth() throws IOException {
        return readDirectoryInt(DIR_GLOBAL_DEPTH_OFFSET);
    }

    private void setGlobalDepth(int globalDepth) throws IOException {
        writeDirectoryInt(DIR_GLOBAL_DEPTH_OFFSET, globalDepth);
    }

    private int getNextBucketId() throws IOException {
        return readDirectoryInt(DIR_NEXT_BUCKET_ID_OFFSET);
    }

    private void setNextBucketId(int nextBucketId) throws IOException {
        writeDirectoryInt(DIR_NEXT_BUCKET_ID_OFFSET, nextBucketId);
    }

    private int getDirectoryEntry(int index) throws IOException {
        return readDirectoryInt(directoryOffset(index));
    }

    private void setDirectoryEntry(int index, int bucketId) throws IOException {
        writeDirectoryInt(directoryOffset(index), bucketId);
    }

    private int readDirectoryInt(int offset) throws IOException {
        return directoryHandle.buffer.getInt(offset);
    }

    private void writeDirectoryInt(int offset, int value) throws IOException {
        directoryHandle.buffer.putInt(offset, value);
    }

    private int directoryOffset(int index) {
        return DIRECTORY_HEADER_SIZE + index * INT_SIZE;
    }

    private int recordOffset(int index) {
        return BUCKET_HEADER_SIZE + index * RECORD_SIZE;
    }

    private Path bucketPath(int bucketId) {
        return bucketsDir.resolve("bucket_" + bucketId + ".dat");
    }

    private void withBucket(int bucketId, IOBufferConsumer action) throws IOException {
        action.accept(bucketHandle(bucketId).buffer);
    }

    private <T> T withBucketResult(int bucketId, IOBufferFunction<T> action) throws IOException {
        return action.apply(bucketHandle(bucketId).buffer);
    }

    private FileHandle bucketHandle(int bucketId) throws IOException {
        FileHandle handle = bucketHandles.get(bucketId);
        if (handle != null) {
            return handle;
        }

        handle = openFileHandle(bucketPath(bucketId), bucketFileSize);
        bucketHandles.put(bucketId, handle);
        return handle;
    }

    private FileHandle openFileHandle(Path path, int size) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        try {
            raf.setLength(size);
            FileChannel channel = raf.getChannel();
            try {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
                return new FileHandle(raf, channel, buffer);
            } catch (IOException e) {
                channel.close();
                throw e;
            }
        } catch (IOException e) {
            raf.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;

        for (FileHandle handle : bucketHandles.values()) {
            try {
                handle.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        bucketHandles.clear();

        try {
            directoryHandle.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    @FunctionalInterface
    private interface IOBufferConsumer {
        void accept(MappedByteBuffer buffer) throws IOException;
    }

    @FunctionalInterface
    private interface IOBufferFunction<T> {
        T apply(MappedByteBuffer buffer) throws IOException;
    }

    private static class Bucket {
        final int localDepth;
        final int size;

        Bucket(int localDepth, int size) {
            this.localDepth = localDepth;
            this.size = size;
        }
    }

    private static class Record {
        final byte used;
        final long key;
        final long value;

        Record(byte used, long key, long value) {
            this.used = used;
            this.key = key;
            this.value = value;
        }
    }

    private static class FileHandle implements AutoCloseable {
        final RandomAccessFile raf;
        final FileChannel channel;
        final MappedByteBuffer buffer;

        FileHandle(RandomAccessFile raf, FileChannel channel, MappedByteBuffer buffer) {
            this.raf = raf;
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void close() throws IOException {
            channel.close();
            raf.close();
        }
    }

    public static void main(String[] args) throws IOException {
        ExtendibleHashTable table =
                new ExtendibleHashTable(Paths.get("storage"), 2);

        table.put(10L, 100L);
        table.put(20L, 200L);
        table.put(30L, 300L);
        table.put(40L, 400L);

        System.out.println(table.get(10L));
        System.out.println(table.get(20L));

        table.update(20L, 999L);
        System.out.println(table.get(20L));

        table.remove(30L);
        System.out.println(table.get(30L));

        table.printState();
    }
}
