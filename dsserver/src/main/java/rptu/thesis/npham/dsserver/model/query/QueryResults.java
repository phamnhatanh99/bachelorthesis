package rptu.thesis.npham.dsserver.model.query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public record QueryResults(List<SingleResult> results) {

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        results.forEach(r -> builder.append(r.toString()).append("\n"));
        return builder.toString();
    }

    public void add(String query, String candidate, double score) {
        results.add(new SingleResult(query, candidate, score));
    }

    public void addAll(QueryResults results) {
        this.results.addAll(results.results());
    }

    public int size() {
        return results.size();
    }

    /**
     * Returns the results sorted by score in descending order
     */
    public void sortResults() {
        results.sort((r1, r2) -> Double.compare(r2.score(), r1.score()));
    }

    public void limitResults(int limit) {
        sortResults();
        results.subList(limit, results.size()).clear();
    }

    public QueryResults withThreshold(double threshold) {
        return new QueryResults(results.stream().filter(r -> r.score() >= threshold).collect(Collectors.toCollection(ArrayList::new)));
    }
}
