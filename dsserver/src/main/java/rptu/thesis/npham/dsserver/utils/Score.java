package rptu.thesis.npham.dsserver.utils;

public record Score(double js, double jcx, double jcy) implements Comparable<Score> {

        // Compare similarity by containment first
        @Override
        public int compareTo(Score o) {
            return Double.compare(jcx, o.jcx);
        }
}
