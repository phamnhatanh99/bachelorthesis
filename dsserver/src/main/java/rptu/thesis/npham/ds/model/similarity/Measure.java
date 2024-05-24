package rptu.thesis.npham.ds.model.similarity;

public record Measure(MeasureEnum measures, double score, double weight) {
    @Override
    public String toString() {
        return measures + "= " + score;
    }
}
