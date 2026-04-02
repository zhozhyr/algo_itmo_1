package org.example;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExtendibleHashTableRandomizedTest {

    private static final int BUCKET_CAPACITY = 2;
    private static final int KEY_BOUND = 200;
    private static final int STEPS = 2_000;

    @RepeatedTest(1)
    void randomPutsPreserveLatestValuesAcrossReopen(@TempDir Path tempDir) throws IOException {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        ExtendibleHashTable table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
        Map<Long, Long> expected = new HashMap<>();

        for (int step = 0; step < STEPS; step++) {
            long key = random.nextInt(KEY_BOUND);
            long value = random.nextLong(10_000);

            table.put(key, value);
            expected.put(key, value);

            verifySnapshot(table, expected, random, seed, step);
            table = reopenIfNeeded(table, tempDir, expected, seed, step);
        }

        verifyFullState(table, expected, seed, STEPS);
        table.close();
    }

    @RepeatedTest(1)
    void randomUpdatesMatchHashMap(@TempDir Path tempDir) throws IOException {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        ExtendibleHashTable table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
        Map<Long, Long> expected = prefillTable(table, random);

        for (int step = 0; step < STEPS; step++) {
            long key = random.nextInt(KEY_BOUND);
            long value = random.nextLong(10_000);

            boolean actualUpdated = table.update(key, value);
            boolean expectedUpdated = expected.containsKey(key);
            if (expectedUpdated) {
                expected.put(key, value);
            }
            assertEquals(
                    expectedUpdated,
                    actualUpdated,
                    failureContext(seed, step, "update(" + key + ", " + value + ")")
            );

            verifySnapshot(table, expected, random, seed, step);
            table = reopenIfNeeded(table, tempDir, expected, seed, step);
        }

        verifyFullState(table, expected, seed, STEPS);
        table.close();
    }

    @RepeatedTest(1)
    void randomRemovesMatchHashMap(@TempDir Path tempDir) throws IOException {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        ExtendibleHashTable table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
        Map<Long, Long> expected = prefillTable(table, random);

        for (int step = 0; step < STEPS; step++) {
            long key = random.nextInt(KEY_BOUND);

            boolean actualRemoved = table.remove(key);
            boolean expectedRemoved = expected.remove(key) != null;
            assertEquals(
                    expectedRemoved,
                    actualRemoved,
                    failureContext(seed, step, "remove(" + key + ")")
            );

            verifySnapshot(table, expected, random, seed, step);
            table = reopenIfNeeded(table, tempDir, expected, seed, step);
        }

        verifyFullState(table, expected, seed, STEPS);
        table.close();
    }

    @RepeatedTest(1)
    void randomMixedOperationsMatchHashMap(@TempDir Path tempDir) throws IOException {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        ExtendibleHashTable table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
        Map<Long, Long> expected = new HashMap<>();

        TableAction[] actions = {
                this::applyPut,
                this::applyUpdate,
                this::applyRemove,
                this::applyGet
        };

        for (int step = 0; step < STEPS; step++) {
            long key = random.nextInt(KEY_BOUND);
            long value = random.nextLong(10_000);
            TableAction action = actions[random.nextInt(actions.length)];

            action.apply(table, expected, key, value, seed, step);
            verifySnapshot(table, expected, random, seed, step);
            table = reopenIfNeeded(table, tempDir, expected, seed, step);
        }

        verifyFullState(table, expected, seed, STEPS);
        table.close();
    }

    @Test
    void putBehavesLikeInsertOrReplaceAcrossReopen(@TempDir Path tempDir) throws IOException {
        ExtendibleHashTable table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);

        table.put(42L, 100L);
        assertEquals(100L, table.get(42L));

        table.put(42L, 200L);
        assertEquals(200L, table.get(42L));

        table.close();
        table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
        assertEquals(200L, table.get(42L));

        assertTrue(table.update(42L, 300L));
        assertEquals(300L, table.get(42L));

        assertTrue(table.remove(42L));
        assertNull(table.get(42L));
        assertFalse(table.remove(42L));
        table.close();
    }

    private void applyPut(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            long key,
            long value,
            long seed,
            int step
    ) throws IOException {
        table.put(key, value);
        expected.put(key, value);
    }

    private void applyUpdate(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            long key,
            long value,
            long seed,
            int step
    ) throws IOException {
        boolean actualUpdated = table.update(key, value);
        boolean expectedUpdated = expected.containsKey(key);
        if (expectedUpdated) {
            expected.put(key, value);
        }
        assertEquals(
                expectedUpdated,
                actualUpdated,
                failureContext(seed, step, "update(" + key + ", " + value + ")")
        );
    }

    private void applyRemove(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            long key,
            long value,
            long seed,
            int step
    ) throws IOException {
        boolean actualRemoved = table.remove(key);
        boolean expectedRemoved = expected.remove(key) != null;
        assertEquals(
                expectedRemoved,
                actualRemoved,
                failureContext(seed, step, "remove(" + key + ")")
        );
    }

    private void applyGet(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            long key,
            long value,
            long seed,
            int step
    ) throws IOException {
        assertEquals(
                expected.get(key),
                table.get(key),
                failureContext(seed, step, "get(" + key + ")")
        );
    }

    private static Map<Long, Long> prefillTable(ExtendibleHashTable table, Random random) throws IOException {
        Map<Long, Long> expected = new HashMap<>();
        for (long key = 0; key < KEY_BOUND; key++) {
            long value = random.nextLong(10_000);
            table.put(key, value);
            expected.put(key, value);
        }
        return expected;
    }

    private static ExtendibleHashTable reopenIfNeeded(
            ExtendibleHashTable table,
            Path tempDir,
            Map<Long, Long> expected,
            long seed,
            int step
    ) throws IOException {
        if (step > 0 && step % 250 == 0) {
            table.close();
            table = new ExtendibleHashTable(tempDir, BUCKET_CAPACITY);
            verifyFullState(table, expected, seed, step);
        }
        return table;
    }

    private static void verifySnapshot(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            Random random,
            long seed,
            int step
    ) throws IOException {
        for (int i = 0; i < 10; i++) {
            long probeKey = random.nextInt(KEY_BOUND);
            assertEquals(
                    expected.get(probeKey),
                    table.get(probeKey),
                    failureContext(seed, step, "snapshot get(" + probeKey + ")")
            );
        }
    }

    private static void verifyFullState(
            ExtendibleHashTable table,
            Map<Long, Long> expected,
            long seed,
            int step
    ) throws IOException {
        for (long key = 0; key < KEY_BOUND; key++) {
            assertEquals(
                    expected.get(key),
                    table.get(key),
                    failureContext(seed, step, "full verification get(" + key + ")")
            );
        }
    }

    private static String failureContext(long seed, int step, String operation) {
        return "seed=" + seed + ", step=" + step + ", operation=" + operation;
    }

    @FunctionalInterface
    private interface TableAction {
        void apply(
                ExtendibleHashTable table,
                Map<Long, Long> expected,
                long key,
                long value,
                long seed,
                int step
        ) throws IOException;
    }
}
