package rptu.thesis.npham.dscommon.model.sketch;

import java.util.Arrays;

public record Sketch(SketchType type, long cardinality, long[] hash_values) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sketch s)) return false;
        return type.equals(s.type()) && cardinality == s.cardinality() && arrayEquals(hash_values, s.hash_values);
    }

    private boolean arrayEquals(long[] a1, long[] a2) {
        long[] a1_sorted = Arrays.copyOf(a1, a1.length);
        Arrays.sort(a1_sorted);
        long[] a2_sorted = Arrays.copyOf(a2, a2.length);
        Arrays.sort(a2_sorted);
        return Arrays.equals(a1_sorted, a2_sorted);
    }
}
