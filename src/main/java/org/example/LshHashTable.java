package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class LshHashTable {

    private final int dimension;
    private final int numHashFunctions;
    private final double[][] planes;
    private final Map<Long, List<Integer>> table = new HashMap<>();
    private final List<double[]> vectors = new ArrayList<>();
    private final List<String> texts = new ArrayList<>();

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
            for (int coordinate = 0; coordinate < dimension; coordinate++) {
                planes[hashIndex][coordinate] = random.nextGaussian();
            }
        }
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
        texts.add(null);

        table.computeIfAbsent(hash(stored), ignored -> new ArrayList<>()).add(id);
        return id;
    }

    public int addText(String text) {
        Objects.requireNonNull(text, "text must not be null");
        int id = add(toTextVector(text, dimension));
        texts.set(id, text);
        return id;
    }

    public List<List<Integer>> read() {
        if (vectors.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<Integer>> buckets = new ArrayList<>();
        for (List<Integer> bucket : table.values()) {
            if (!bucket.isEmpty()) {
                buckets.add(List.copyOf(bucket));
            }
        }
        return buckets;
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

    public List<int[]> findTextDoublesByFullScan() {
        List<int[]> pairs = new ArrayList<>();
        for (int left = 0; left < texts.size(); left++) {
            String leftText = texts.get(left);
            if (leftText == null) {
                continue;
            }
            for (int right = left + 1; right < texts.size(); right++) {
                if (leftText.equals(texts.get(right))) {
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

    public String getText(int id) {
        if (id < 0 || id >= texts.size() || texts.get(id) == null) {
            throw new IndexOutOfBoundsException("Invalid text id: " + id);
        }
        return texts.get(id);
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
            for (int coordinate = 0; coordinate < dimension; coordinate++) {
                dot += vector[coordinate] * planes[hashIndex][coordinate];
            }
            if (dot >= 0.0) {
                bits |= 1L << hashIndex;
            }
        }
        return bits;
    }

    public static double[] toTextVector(String text, int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }

        double[] vector = new double[dimension];
        String normalized = normalizeText(text);

        if (normalized.isEmpty()) {
            return vector;
        }

        if (normalized.length() <= 3) {
            vector[Math.floorMod(normalized.hashCode(), dimension)] += 1.0;
            return vector;
        }

        for (int start = 0; start <= normalized.length() - 3; start++) {
            String shingle = normalized.substring(start, start + 3);
            vector[Math.floorMod(shingle.hashCode(), dimension)] += 1.0;
        }

        return vector;
    }

    private static String normalizeText(String text) {
        return text
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ")
                .trim();
    }
}
