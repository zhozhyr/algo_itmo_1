package benchmark;

import org.example.ExtendibleHashTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
@Threads(1)
public class ExtendibleHashTableSimpleLatencyBenchmark {

    private static final int BUCKET_CAPACITY = 32;
    private static final int BATCH_OPERATIONS = 256;
    private static final int UPDATE_BATCH_OPERATIONS = 4096;
    private static final int INSERT_BASE_DIVISOR = 2;
    private static final int MAX_ITEM_COUNT = 12_000;
    private static final int INSERT_POOL_SIZE = MAX_ITEM_COUNT + BATCH_OPERATIONS * 64;

    private static final long[] READ_KEY_POOL = keyPool(MAX_ITEM_COUNT);
    private static final long[] UPDATE_KEY_POOL = keyPool(MAX_ITEM_COUNT);
    private static final long[] INSERT_KEY_POOL = shuffledKeyPool(INSERT_POOL_SIZE, 3_001L);
    private static final long[] DELETE_KEY_POOL = keyPool(MAX_ITEM_COUNT);

    @State(Scope.Benchmark)
    public static class ReadState {

        @Param({"2000", "4000", "6000", "8000", "10000", "12000"})
        public int itemCount;

        Path directory;
        ExtendibleHashTable table;
        int[] queryIndexes;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("jmh-eht-read");
            table = new ExtendibleHashTable(directory, BUCKET_CAPACITY);
            queryIndexes = randomIndexes(itemCount, BATCH_OPERATIONS, 17_001L);

            for (int index = 0; index < itemCount; index++) {
                table.put(READ_KEY_POOL[index], valueOf(index));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (table != null) {
                table.close();
                table = null;
            }
            deleteDirectory(directory);
        }
    }

    @State(Scope.Benchmark)
    public static class UpdateState {

        @Param({"2000", "4000", "6000", "8000", "10000", "12000"})
        public int itemCount;

        Path directory;
        ExtendibleHashTable table;
        int[] updateIndexes;
        long[] updatedValues;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("jmh-eht-update");
            table = new ExtendibleHashTable(directory, BUCKET_CAPACITY);
            updateIndexes = randomIndexes(itemCount, UPDATE_BATCH_OPERATIONS, 27_001L);
            updatedValues = values("updated", UPDATE_BATCH_OPERATIONS);

            for (int index = 0; index < itemCount; index++) {
                table.put(UPDATE_KEY_POOL[index], valueOf(index));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (table != null) {
                table.close();
                table = null;
            }
            deleteDirectory(directory);
        }
    }

    @State(Scope.Benchmark)
    public static class InsertState {

        @Param({"2000", "4000", "6000", "8000", "10000", "12000"})
        public int itemCount;

        Path directory;
        ExtendibleHashTable table;
        long[] insertedValues;
        int insertWindowOffset;
        int preloadedItemCount;

        @Setup(Level.Invocation)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("jmh-eht-insert");
            table = new ExtendibleHashTable(directory, BUCKET_CAPACITY);
            insertedValues = values("inserted", BATCH_OPERATIONS);
            preloadedItemCount = preloadedItemCount(itemCount);
            insertWindowOffset = insertWindowOffset(preloadedItemCount);

            for (int index = 0; index < preloadedItemCount; index++) {
                table.put(INSERT_KEY_POOL[insertWindowOffset + index], valueOf(index));
            }
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            if (table != null) {
                table.close();
                table = null;
            }
            deleteDirectory(directory);
        }
    }

    @State(Scope.Benchmark)
    public static class DeleteState {

        @Param({"2000", "4000", "6000", "8000", "10000", "12000"})
        public int itemCount;

        Path directory;
        ExtendibleHashTable table;
        int[] deleteIndexes;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("jmh-eht-delete");
            table = new ExtendibleHashTable(directory, BUCKET_CAPACITY);
            deleteIndexes = randomIndexes(itemCount, BATCH_OPERATIONS, 47_001L);

            for (int index = 0; index < itemCount; index++) {
                table.put(DELETE_KEY_POOL[index], valueOf(index));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            if (table != null) {
                table.close();
                table = null;
            }
            deleteDirectory(directory);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(BATCH_OPERATIONS)
    public void getExistingBatch(ReadState state, Blackhole blackhole) throws IOException {
        for (int index : state.queryIndexes) {
            blackhole.consume(state.table.get(READ_KEY_POOL[index]));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(UPDATE_BATCH_OPERATIONS)
    public void updateExistingBatch(UpdateState state) throws IOException {
        for (int position = 0; position < state.updateIndexes.length; position++) {
            state.table.update(
                    UPDATE_KEY_POOL[state.updateIndexes[position]],
                    state.updatedValues[position]
            );
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(BATCH_OPERATIONS)
    public void insertFreshBatch(InsertState state) throws IOException {
        for (int index = 0; index < BATCH_OPERATIONS; index++) {
            state.table.put(
                    INSERT_KEY_POOL[state.insertWindowOffset + state.preloadedItemCount + index],
                    state.insertedValues[index]
            );
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(BATCH_OPERATIONS)
    public void deleteExistingBatch(DeleteState state) throws IOException {
        for (int index : state.deleteIndexes) {
            state.table.remove(DELETE_KEY_POOL[index]);
        }
    }

    private static long[] keyPool(int size) {
        long[] keys = new long[size];
        for (int index = 0; index < size; index++) {
            keys[index] = index;
        }
        return keys;
    }

    private static long[] shuffledKeyPool(int size, long seed) {
        long[] keys = keyPool(size);
        Random random = new Random(seed);
        for (int index = size - 1; index > 0; index--) {
            int swapIndex = random.nextInt(index + 1);
            long tmp = keys[index];
            keys[index] = keys[swapIndex];
            keys[swapIndex] = tmp;
        }
        return keys;
    }

    private static int insertWindowOffset(int itemCount) {
        int windowSize = itemCount + BATCH_OPERATIONS;
        int maxOffset = INSERT_KEY_POOL.length - windowSize;
        if (maxOffset <= 0) {
            return 0;
        }
        return (int) ((itemCount * 997L) % (maxOffset + 1L));
    }

    private static int preloadedItemCount(int itemCount) {
        return Math.max(1, itemCount / INSERT_BASE_DIVISOR);
    }

    private static int[] randomIndexes(int bound, int size, long seed) {
        java.util.Random random = new java.util.Random(seed);
        int[] indexes = new int[size];
        for (int i = 0; i < size; i++) {
            indexes[i] = random.nextInt(bound);
        }
        return indexes;
    }

    private static long[] values(String prefix, int size) {
        long[] values = new long[size];
        long salt = prefix.hashCode();
        for (int i = 0; i < size; i++) {
            values[i] = salt + i;
        }
        return values;
    }

    private static long valueOf(int index) {
        return index;
    }

    private static void deleteDirectory(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }

        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw e;
        }
    }
}
