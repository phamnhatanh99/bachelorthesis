package rptu.thesis.npham.dsserver.model.similarity;

public record Measure(MeasureType measures, double score, int weight) {
    @Override
    public String toString() {
        return measures + "= " + score;
    }
}
