package org.example;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public final class PerfectHashTable<K> {

    private static final int MAX_HASH_VALUE = Integer.MAX_VALUE;
    private static final int MAX_PRIMARY_ATTEMPTS = 10_000;
    private static final int MAX_SECONDARY_ATTEMPTS = 10_000;

    private final int size;
    private final SecondaryTable<K>[] primaryTable;
    private final UniversalHashFunction primaryHash;

    public PerfectHashTable(Collection<? extends K> keys) {
        Objects.requireNonNull(keys, "keys must not be null");

        this.size = keys.size();
        if (size == 0) {
            this.primaryTable = emptyPrimaryTable();
            this.primaryHash = UniversalHashFunction.identity();
            return;
        }

        List<Entry<K>> entries = new ArrayList<>(size);
        int index = 0;
        for (K key : keys) {
            entries.add(new Entry<>(key, index++));
        }

        ensureUniqueKeys(entries);

        Random random = new Random(0x5EEDC0DEL ^ size);
        PrimaryBuildResult<K> result = buildPrimary(entries, random);
        this.primaryTable = result.primaryTable;
        this.primaryHash = result.primaryHash;
    }

    public boolean contains(K key) {
        return indexOf(key) >= 0;
    }

    public int indexOf(K key) {
        if (size == 0) {
            return -1;
        }

        SecondaryTable<K> secondary = primaryTable[primaryHash.index(bucketHash(key), primaryTable.length)];
        if (secondary == null || secondary.size == 0) {
            return -1;
        }

        int slot = secondary.hash.index(bucketHash(key), secondary.slots.length);
        Entry<K> entry = secondary.slots[slot];
        if (entry != null && Objects.equals(entry.key, key)) {
            return entry.index;
        }
        return -1;
    }

    public int size() {
        return size;
    }

    private static <K> void ensureUniqueKeys(List<Entry<K>> entries) {
        int n = entries.size();
        if (n <= 1) {
            return;
        }

        UniversalHashFunction hash = UniversalHashFunction.identity();
        @SuppressWarnings("unchecked")
        List<Entry<K>>[] buckets = new List[n];
        for (Entry<K> entry : entries) {
            int bucket = hash.index(bucketHash(entry.key), n);
            List<Entry<K>> list = buckets[bucket];
            if (list == null) {
                list = new ArrayList<>();
                buckets[bucket] = list;
            }

            for (Entry<K> existing : list) {
                if (Objects.equals(existing.key, entry.key)) {
                    throw new IllegalArgumentException("Duplicate key: " + entry.key);
                }
            }
            list.add(entry);
        }
    }

    private static <K> PrimaryBuildResult<K> buildPrimary(List<Entry<K>> entries, Random random) {
        int n = entries.size();

        for (int attempt = 0; attempt < MAX_PRIMARY_ATTEMPTS; attempt++) {
            UniversalHashFunction primaryHash = UniversalHashFunction.random(random);
            @SuppressWarnings("unchecked")
            List<Entry<K>>[] buckets = new List[n];

            for (Entry<K> entry : entries) {
                int bucket = primaryHash.index(bucketHash(entry.key), n);
                List<Entry<K>> list = buckets[bucket];
                if (list == null) {
                    list = new ArrayList<>();
                    buckets[bucket] = list;
                }
                list.add(entry);
            }

            int secondarySlots = 0;
            for (List<Entry<K>> bucket : buckets) {
                if (bucket != null) {
                    secondarySlots += bucket.size() * bucket.size();
                }
            }

            if (secondarySlots > 4 * n) {
                continue;
            }

            @SuppressWarnings("unchecked")
            SecondaryTable<K>[] primaryTable = new SecondaryTable[n];
            for (int i = 0; i < n; i++) {
                List<Entry<K>> bucket = buckets[i];
                primaryTable[i] = bucket == null
                        ? SecondaryTable.empty()
                        : buildSecondary(bucket, random);
            }
            return new PrimaryBuildResult<>(primaryTable, primaryHash);
        }

        throw new IllegalStateException("Failed to build perfect hash table");
    }

    private static <K> SecondaryTable<K> buildSecondary(List<Entry<K>> bucket, Random random) {
        int bucketSize = bucket.size();
        if (bucketSize == 1) {
            @SuppressWarnings("unchecked")
            Entry<K>[] slots = (Entry<K>[]) new Entry<?>[1];
            slots[0] = bucket.getFirst();
            return new SecondaryTable<>(slots, UniversalHashFunction.identity(), 1);
        }

        int secondarySize = bucketSize * bucketSize;
        for (int attempt = 0; attempt < MAX_SECONDARY_ATTEMPTS; attempt++) {
            UniversalHashFunction hash = UniversalHashFunction.random(random);
            @SuppressWarnings("unchecked")
            Entry<K>[] slots = (Entry<K>[]) new Entry<?>[secondarySize];
            boolean collision = false;

            for (Entry<K> entry : bucket) {
                int slot = hash.index(bucketHash(entry.key), secondarySize);
                if (slots[slot] != null) {
                    collision = true;
                    break;
                }
                slots[slot] = entry;
            }

            if (!collision) {
                return new SecondaryTable<>(slots, hash, bucketSize);
            }
        }

        throw new IllegalStateException("Failed to build collision-free secondary table");
    }

    @SuppressWarnings("unchecked")
    private static <K> SecondaryTable<K>[] emptyPrimaryTable() {
        return (SecondaryTable<K>[]) new SecondaryTable<?>[0];
    }

    private static int bucketHash(Object key) {
        return spread(Objects.hashCode(key));
    }

    private static int spread(int hash) {
        hash ^= (hash >>> 16);
        return hash & Integer.MAX_VALUE;
    }

    private record Entry<K>(K key, int index) {
    }

    private record PrimaryBuildResult<K>(
            SecondaryTable<K>[] primaryTable,
            UniversalHashFunction primaryHash
    ) {
    }

    private record SecondaryTable<K>(
            Entry<K>[] slots,
            UniversalHashFunction hash,
            int size
    ) {
        private static <K> SecondaryTable<K> empty() {
            @SuppressWarnings("unchecked")
            Entry<K>[] slots = (Entry<K>[]) new Entry<?>[0];
            return new SecondaryTable<>(slots, UniversalHashFunction.identity(), 0);
        }
    }

    private record UniversalHashFunction(int a, int b) {
        private static UniversalHashFunction random(Random random) {
            int a = random.nextInt(MAX_HASH_VALUE - 1) + 1;
            int b = random.nextInt(MAX_HASH_VALUE);
            return new UniversalHashFunction(a, b);
        }

        private static UniversalHashFunction identity() {
            return new UniversalHashFunction(1, 0);
        }

        private int index(int value, int mod) {
            if (mod == 0) {
                return 0;
            }

            return hash(value) % mod;
        }

        private int hash(int value) {
            return (int) Math.floorMod((long) a * value + b, MAX_HASH_VALUE);
        }
    }
}
