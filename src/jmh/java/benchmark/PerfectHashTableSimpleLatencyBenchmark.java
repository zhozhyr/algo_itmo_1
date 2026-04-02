package benchmark;

import org.example.PerfectHashTable;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
@Threads(1)
public class PerfectHashTableSimpleLatencyBenchmark {

    private static final int BATCH_OPERATIONS = 256;
    private static final int MAX_ITEM_COUNT = 10_000;

    private static final long[] KEY_POOL = keyPool(MAX_ITEM_COUNT);

    @State(Scope.Benchmark)
    public static class ReadState {

        @Param({"1000", "2500", "5000", "7500", "10000"})
        public int itemCount;

        PerfectHashTable<Long> table;
        int[] queryIndexes;

        @Setup(Level.Iteration)
        public void setUp() {
            table = new PerfectHashTable<>(keys(itemCount));
            queryIndexes = randomIndexes(itemCount, BATCH_OPERATIONS, 17_001L);
        }
    }

    @State(Scope.Benchmark)
    public static class BuildState {

        @Param({"1000", "2500", "5000", "7500", "10000"})
        public int itemCount;

        List<Long> keys;

        @Setup(Level.Invocation)
        public void setUp() {
            keys = keys(itemCount);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(BATCH_OPERATIONS)
    public void getExistingBatch(ReadState state, Blackhole blackhole) {
        for (int index : state.queryIndexes) {
            blackhole.consume(state.table.indexOf(KEY_POOL[index]));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void buildFullIndex(BuildState state, Blackhole blackhole) {
        blackhole.consume(new PerfectHashTable<>(state.keys));
    }

    private static List<Long> keys(int itemCount) {
        List<Long> keys = new ArrayList<>(itemCount);
        for (int index = 0; index < itemCount; index++) {
            keys.add(KEY_POOL[index]);
        }
        return keys;
    }

    private static long[] keyPool(int size) {
        long[] keys = new long[size];
        for (int index = 0; index < size; index++) {
            keys[index] = index;
        }
        return keys;
    }

    private static int[] randomIndexes(int bound, int size, long seed) {
        java.util.Random random = new java.util.Random(seed);
        int[] indexes = new int[size];
        for (int i = 0; i < size; i++) {
            indexes[i] = random.nextInt(bound);
        }
        return indexes;
    }
}
