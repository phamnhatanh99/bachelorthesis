package rptu.thesis.npham.dscommon.model.query;

import rptu.thesis.npham.dscommon.model.metadata.Metadata;

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

    /**
     * Add a new result to the result lists
     */
    public void add(Metadata query, Metadata candidate, double score) {
        results.add(new SingleResult(query, candidate, score));
    }

    /**
     * Add all results from another QueryResults object to this object
     */
    public void addAll(QueryResults results) {
        this.results.addAll(results.results());
    }

    /**
     * Return a new QueryResults object with the results sorted by score in descending order
     */
    public QueryResults sortResults() {
        return new QueryResults(results.stream()
                .sorted((r1, r2) -> Double.compare(r2.score(), r1.score()))
                .collect(Collectors.toCollection(ArrayList::new)));
    }

    /**
     * Return a new QueryResults object with the top n highest scoring results. The results are sorted by score in descending order
     */
    public QueryResults limitResults(int limit) {
        return new QueryResults(results.stream()
                .sorted((r1, r2) -> Double.compare(r2.score(), r1.score()))
                .limit(limit)
                .collect(Collectors.toCollection(ArrayList::new)));
    }

    /**
     * Return a new QueryResults object with only the results that have a score greater than or equal to the threshold
     */
    public QueryResults withThreshold(double threshold) {
        return new QueryResults(results.stream()
                .filter(r -> r.score() >= threshold)
                .collect(Collectors.toCollection(ArrayList::new)));
    }
}
