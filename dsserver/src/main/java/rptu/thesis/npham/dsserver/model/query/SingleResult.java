package rptu.thesis.npham.dsserver.model.query;

public record SingleResult(String query, String candidate, double score) {
    @Override
    public String toString() {
        return query + " is similar to " + candidate + " with a score of " + score;
    }
}
