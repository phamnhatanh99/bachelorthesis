package rptu.thesis.npham.ds.model.similarity;

public record Measure(SimilarityMeasures similarity_measure, double score) {
    @Override
    public String toString() {
        return similarity_measure + "= " + score;
    }
}
