package rptu.thesis.npham.ds.utils;

public record Pair<A, B>(A first, B second) {
    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
