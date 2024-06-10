package rptu.thesis.npham.dsserver.model.similarity;

public record Measure(MeasureType measure, double score, int weight) {
    @Override
    public String toString() {
        return measure + "= " + score;
    }
}
