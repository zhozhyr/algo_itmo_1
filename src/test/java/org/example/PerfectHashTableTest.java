package org.example;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PerfectHashTableTest {

    @Test
    void keepsInsertionIndexForFixedKeySet() {
        List<Long> keys = List.of(10L, 42L, -7L, 123_456_789L);

        PerfectHashTable<Long> table = new PerfectHashTable<>(keys);

        for (int i = 0; i < keys.size(); i++) {
            assertTrue(table.contains(keys.get(i)));
            assertEquals(i, table.indexOf(keys.get(i)));
        }
        assertFalse(table.contains(11L));
        assertEquals(-1, table.indexOf(11L));
        assertEquals(keys.size(), table.size());
    }

    @Test
    void supportsEmptySet() {
        PerfectHashTable<Long> table = new PerfectHashTable<>(List.of());

        assertEquals(0, table.size());
        assertFalse(table.contains(1L));
        assertEquals(-1, table.indexOf(1L));
    }

    @Test
    void rejectsDuplicateKeys() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PerfectHashTable<>(List.of(5L, 7L, 5L))
        );
    }

    @Test
    void randomUniqueSetsAreIndexedWithoutMisses() {
        Random random = new Random(42);

        for (int iteration = 0; iteration < 200; iteration++) {
            Set<Long> uniqueKeys = new LinkedHashSet<>();
            while (uniqueKeys.size() < 150) {
                uniqueKeys.add(random.nextLong(100_000));
            }

            List<Long> keys = new ArrayList<>(uniqueKeys);
            PerfectHashTable<Long> table = new PerfectHashTable<>(keys);

            for (int i = 0; i < keys.size(); i++) {
                assertEquals(i, table.indexOf(keys.get(i)), "iteration=" + iteration + ", key=" + keys.get(i));
            }

            for (int i = 0; i < 100; i++) {
                long probe = 200_000L + random.nextLong(100_000);
                assertEquals(-1, table.indexOf(probe), "iteration=" + iteration + ", probe=" + probe);
            }
        }
    }
}
