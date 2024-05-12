package rptu.thesis.npham.ds.model;

import java.util.Arrays;

public class Sketch {

    private String type;
    private long cardinality;
    private long[] hash_values;

    public Sketch() {}

    public Sketch(String type, long cardinality, long[] hash_values) {
        this.type = type;
        this.cardinality = cardinality;
        this.hash_values = hash_values;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    public long[] getHashValues() {
        return hash_values;
    }

    public void setHashValues(long[] hash_values) {
        this.hash_values = hash_values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sketch s)) return false;
        return type.equals(s.getType()) && cardinality == s.getCardinality() && arrayEquals(hash_values, s.getHashValues());
    }

    private boolean arrayEquals(long[] a1, long[] a2) {
        long[] a1_sorted = Arrays.copyOf(a1, a1.length);
        Arrays.sort(a1_sorted);
        long[] a2_sorted = Arrays.copyOf(a2, a2.length);
        Arrays.sort(a2_sorted);
        return Arrays.equals(a1_sorted, a2_sorted);
    }
}
