package rptu.thesis.npham.dsserver.model.similarity;

public record Measure(MeasureType measures, double score, double weight) {
    @Override
    public String toString() {
        return measures + "= " + score;
    }
}
