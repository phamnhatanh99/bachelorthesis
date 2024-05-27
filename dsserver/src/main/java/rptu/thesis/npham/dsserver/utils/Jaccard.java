package rptu.thesis.npham.dsserver.utils;

public record Jaccard(double js, double jcx, double jcy) implements Comparable<Jaccard> {

        // Compare similarity by containment first
        @Override
        public int compareTo(Jaccard o) {
            return Double.compare(jcx, o.jcx);
        }
}
