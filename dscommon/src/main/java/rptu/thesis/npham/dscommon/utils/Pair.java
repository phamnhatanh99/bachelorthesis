package rptu.thesis.npham.dscommon.utils;

public record Pair<A, B>(A first, B second) {
    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
