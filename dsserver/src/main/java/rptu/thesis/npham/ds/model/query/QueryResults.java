package rptu.thesis.npham.ds.model.query;

import rptu.thesis.npham.ds.utils.Pair;

import java.util.List;

public record QueryResults(List<Pair<String, Pair<String, Double>>> results) {

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        results.forEach(r ->
                builder.append(r.first()).append(" is similar to ").append(r.second().first()).append(" with a score of ").append(r.second().second()).append("\n"));
        return builder.toString();
    }

    public void add(String query, String candidate, double score) {
        results.add(new Pair<>(query, new Pair<>(candidate, score)));
    }

    public void addAll(List<Pair<String, Pair<String, Double>>> results) {
        this.results.addAll(results);
    }

    public int size() {
        return results.size();
    }

    /**
     * Returns the results sorted by score in descending order
     */
    public void sortResults() {
        results.sort((r1, r2) -> Double.compare(r2.second().second(), r1.second().second()));
    }

    public void limitResults(int limit) {
        results.subList(limit, results.size()).clear();
    }
}
