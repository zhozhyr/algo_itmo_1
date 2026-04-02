package benchmark;

import org.example.LshHashTable;
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

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
@Threads(1)
public class LshHashTableSimpleLatencyBenchmark {

    private static final int DIMENSION = 3;
    private static final int NUM_HASH_FUNCTIONS = 12;
    private static final int BATCH_OPERATIONS = 256;
    private static final int MAX_ITEM_COUNT = 10_000;
    private static final int INSERT_POOL_SIZE = MAX_ITEM_COUNT + BATCH_OPERATIONS * 64;

    private static final double[][] BUILD_POINT_POOL = pointPool(MAX_ITEM_COUNT, 17L);
    private static final double[][] INSERT_POINT_POOL = pointPool(INSERT_POOL_SIZE, 31L);
    private static final double[][] DUPLICATE_SCAN_POOL = duplicatePointPool(MAX_ITEM_COUNT, 53L);

    @State(Scope.Benchmark)
    public static class BuildState {

        @Param({"1000", "2500", "5000", "7500", "10000"})
        public int itemCount;

        double[][] points;

        @Setup(Level.Invocation)
        public void setUp() {
            points = firstPoints(BUILD_POINT_POOL, itemCount);
        }
    }

    @State(Scope.Benchmark)
    public static class InsertState {

        @Param({"1000", "2500", "5000", "7500", "10000"})
        public int itemCount;

        LshHashTable table;
        int insertWindowOffset;
        int invocationIndex;

        @Setup(Level.Invocation)
        public void setUp() {
            table = new LshHashTable(DIMENSION, NUM_HASH_FUNCTIONS);
            insertWindowOffset = insertWindowOffset(itemCount, invocationIndex);
            invocationIndex++;

            for (int index = 0; index < itemCount; index++) {
                table.add(INSERT_POINT_POOL[insertWindowOffset + index]);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class FullScanState {

        @Param({"1000", "2500", "5000", "7500", "10000"})
        public int itemCount;

        LshHashTable table;

        @Setup(Level.Iteration)
        public void setUp() {
            table = new LshHashTable(DIMENSION, NUM_HASH_FUNCTIONS);
            for (int index = 0; index < itemCount; index++) {
                table.add(DUPLICATE_SCAN_POOL[index]);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void buildFullIndex(BuildState state, Blackhole blackhole) {
        LshHashTable table = new LshHashTable(DIMENSION, NUM_HASH_FUNCTIONS);
        for (double[] point : state.points) {
            table.add(point);
        }
        blackhole.consume(table);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @OperationsPerInvocation(BATCH_OPERATIONS)
    public void insertFreshBatch(InsertState state) {
        for (int index = 0; index < BATCH_OPERATIONS; index++) {
            state.table.add(INSERT_POINT_POOL[state.insertWindowOffset + state.itemCount + index]);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void findVectorDoublesByFullScan(FullScanState state, Blackhole blackhole) {
        blackhole.consume(state.table.findVectorDoublesByFullScan());
    }

    private static double[][] firstPoints(double[][] source, int count) {
        double[][] points = new double[count][];
        System.arraycopy(source, 0, points, 0, count);
        return points;
    }

    private static int insertWindowOffset(int itemCount, int invocationIndex) {
        int windowSize = itemCount + BATCH_OPERATIONS;
        int maxOffset = INSERT_POINT_POOL.length - windowSize;
        if (maxOffset <= 0) {
            return 0;
        }
        return (int) ((invocationIndex * 997L) % (maxOffset + 1L));
    }

    private static double[][] pointPool(int size, long seed) {
        java.util.Random random = new java.util.Random(seed);
        double[][] points = new double[size][DIMENSION];
        for (int index = 0; index < size; index++) {
            for (int coordinate = 0; coordinate < DIMENSION; coordinate++) {
                points[index][coordinate] = random.nextDouble(-10_000.0, 10_000.0);
            }
        }
        return points;
    }

    private static double[][] duplicatePointPool(int size, long seed) {
        double[][] uniquePoints = pointPool((size + 1) / 2, seed);
        double[][] points = new double[size][DIMENSION];
        for (int index = 0; index < size; index++) {
            points[index] = uniquePoints[index / 2];
        }
        return points;
    }
}
