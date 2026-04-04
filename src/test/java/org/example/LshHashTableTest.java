package org.example;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LshHashTableTest {

    @Test
    void indexes3dPointsAndFindsDuplicates() {
        LshHashTable table = new LshHashTable(3, 8);

        int first = table.add(new double[]{1.0, 2.0, 3.0});
        int second = table.add(new double[]{-5.0, 0.5, 10.0});
        int duplicateOfFirst = table.add(new double[]{1.0, 2.0, 3.0});
        int duplicateOfSecond = table.add(new double[]{-5.0, 0.5, 10.0});

        assertEquals(4, table.size());
        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, table.getVector(first));
        assertArrayEquals(new double[]{-5.0, 0.5, 10.0}, table.getVector(second));
        assertPairsEqual(
                List.of(
                        new int[]{first, duplicateOfFirst},
                        new int[]{second, duplicateOfSecond}
                ),
                table.findDoubles()
        );
    }

    @Test
    void supportsBulkInitializationAndRead() {
        List<double[]> vectors = List.of(
                new double[]{1.0, 1.0, 1.0},
                new double[]{2.0, 2.0, 2.0},
                new double[]{1.0, 1.0, 1.0}
        );

        LshHashTable table = new LshHashTable(vectors, 10);

        assertEquals(3, table.size());
        assertEquals(3, table.getDimension());
        assertTrue(table.read().stream().anyMatch(bucket -> bucket.contains(0) && bucket.contains(2)));
        assertPairsEqual(List.of(new int[]{0, 2}), table.findDoubles());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new LshHashTable(3, 0));
        assertThrows(IllegalArgumentException.class, () -> new LshHashTable(0, 3));
        assertThrows(IllegalArgumentException.class, () -> new LshHashTable(3, 65));
        assertThrows(NullPointerException.class, () -> new LshHashTable(null, 4));
        assertThrows(IllegalArgumentException.class, () -> new LshHashTable(List.of(), 4));
        assertThrows(IllegalArgumentException.class, () -> new LshHashTable(List.of(new double[0]), 4));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LshHashTable(List.of(new double[]{1.0}, new double[]{1.0, 2.0}), 4)
        );

        LshHashTable table = new LshHashTable(3, 4);
        assertThrows(NullPointerException.class, () -> table.add(null));
        assertThrows(IllegalArgumentException.class, () -> table.add(new double[]{1.0, 2.0}));
        assertThrows(IndexOutOfBoundsException.class, () -> table.getVector(0));
        assertEquals(List.of(), table.findDoubles());
    }

    private static void assertPairsEqual(List<int[]> expected, List<int[]> actual) {
        assertEquals(expected.size(), actual.size());
        List<int[]> expectedSorted = expected.stream()
                .map(pair -> pair.clone())
                .sorted(Comparator.<int[]>comparingInt(pair -> pair[0]).thenComparingInt(pair -> pair[1]))
                .toList();
        List<int[]> actualSorted = actual.stream()
                .map(pair -> pair.clone())
                .sorted(Comparator.<int[]>comparingInt(pair -> pair[0]).thenComparingInt(pair -> pair[1]))
                .toList();
        for (int i = 0; i < expectedSorted.size(); i++) {
            assertArrayEquals(expectedSorted.get(i), actualSorted.get(i));
        }
    }
}
