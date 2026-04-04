package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class LshHashTable {

    private final int dimension;
    private final int numHashFunctions;
    private final double[][] planes;
    private final Map<Long, List<Integer>> table;
    private final List<double[]> vectors = new ArrayList<>();

    public LshHashTable(int dimension, int numHashFunctions) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (numHashFunctions <= 0 || numHashFunctions > Long.SIZE) {
            throw new IllegalArgumentException("numHashFunctions must be in range [1, 64]");
        }

        this.dimension = dimension;
        this.numHashFunctions = numHashFunctions;
        this.planes = new double[numHashFunctions][dimension];

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int hashIndex = 0; hashIndex < numHashFunctions; hashIndex++) {
            double[] plane = planes[hashIndex];
            for (int coordinate = 0; coordinate < dimension; coordinate++) {
                plane[coordinate] = random.nextGaussian();
            }
        }

        this.table = new HashMap<>();
    }

    public LshHashTable(List<double[]> initialVectors, int numHashFunctions) {
        this(requireDimension(initialVectors), numHashFunctions);
        addAll(initialVectors);
    }

    private static int requireDimension(List<double[]> vectors) {
        Objects.requireNonNull(vectors, "vectors must not be null");
        if (vectors.isEmpty()) {
            throw new IllegalArgumentException("vectors must not be empty");
        }

        double[] first = vectors.get(0);
        if (first == null) {
            throw new IllegalArgumentException("First vector is null");
        }

        int dimension = first.length;
        if (dimension == 0) {
            throw new IllegalArgumentException("Vector dimension must be positive");
        }

        for (int index = 1; index < vectors.size(); index++) {
            double[] vector = vectors.get(index);
            if (vector == null) {
                throw new IllegalArgumentException("Vector at index " + index + " is null");
            }
            if (vector.length != dimension) {
                throw new IllegalArgumentException("All vectors must have the same dimension");
            }
        }
        return dimension;
    }

    public int add(double[] vector) {
        Objects.requireNonNull(vector, "vector must not be null");
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension " + vector.length + " does not match index dimension " + dimension
            );
        }

        int id = vectors.size();
        double[] stored = vector.clone();
        vectors.add(stored);

        table.computeIfAbsent(hash(stored), ignored -> new ArrayList<>()).add(id);
        return id;
    }

    public void addAll(List<double[]> newVectors) {
        Objects.requireNonNull(newVectors, "newVectors must not be null");
        for (double[] vector : newVectors) {
            add(vector);
        }
    }

    public List<List<Integer>> read() {
        if (vectors.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<Integer>> buckets = new ArrayList<>();
        for (List<Integer> bucket : table.values()) {
            if (!bucket.isEmpty()) {
                buckets.add(new ArrayList<>(bucket));
            }
        }
        return buckets;
    }

    public List<Integer> lshSearch(double[] queryVector) {
        Objects.requireNonNull(queryVector, "queryVector must not be null");
        if (queryVector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension " + queryVector.length + " does not match index dimension " + dimension
            );
        }

        List<Integer> bucket = table.get(hash(queryVector));
        if (bucket == null || bucket.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(bucket);
    }

    public List<int[]> findDoubles() {
        List<List<Integer>> buckets = read();
        if (buckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<int[]> pairs = new ArrayList<>();
        for (List<Integer> bucket : buckets) {
            int bucketSize = bucket.size();
            if (bucketSize < 2) {
                continue;
            }
            for (int leftIndex = 0; leftIndex < bucketSize; leftIndex++) {
                int leftId = bucket.get(leftIndex);
                double[] leftVector = vectors.get(leftId);
                for (int rightIndex = leftIndex + 1; rightIndex < bucketSize; rightIndex++) {
                    int rightId = bucket.get(rightIndex);
                    if (Arrays.equals(leftVector, vectors.get(rightId))) {
                        pairs.add(new int[]{Math.min(leftId, rightId), Math.max(leftId, rightId)});
                    }
                }
            }
        }
        return pairs;
    }

    public List<int[]> findVectorDoublesByFullScan() {
        List<int[]> pairs = new ArrayList<>();
        for (int left = 0; left < vectors.size(); left++) {
            double[] leftVector = vectors.get(left);
            for (int right = left + 1; right < vectors.size(); right++) {
                if (Arrays.equals(leftVector, vectors.get(right))) {
                    pairs.add(new int[]{left, right});
                }
            }
        }
        return pairs;
    }

    public double[] getVector(int id) {
        if (id < 0 || id >= vectors.size()) {
            throw new IndexOutOfBoundsException("Invalid vector id: " + id);
        }
        return vectors.get(id).clone();
    }

    public int size() {
        return vectors.size();
    }

    public int getDimension() {
        return dimension;
    }

    private long hash(double[] vector) {
        long bits = 0L;
        for (int hashIndex = 0; hashIndex < numHashFunctions; hashIndex++) {
            double dot = 0.0;
            double[] plane = planes[hashIndex];
            for (int coordinate = 0; coordinate < dimension; coordinate++) {
                dot += vector[coordinate] * plane[coordinate];
            }
            if (dot >= 0.0) {
                bits |= 1L << hashIndex;
            }
        }
        return bits;
    }
}
