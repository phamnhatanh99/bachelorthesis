package rptu.thesis.npham.ds.model.similarity;

import rptu.thesis.npham.ds.exceptions.MeasureAlreadyExistException;

import java.util.ArrayList;
import java.util.List;

public class Score implements Comparable<Score> {

    private List<Measure> similarity_measures;

    public Score(List<Measure> similarity_measures) {
        this.similarity_measures = similarity_measures;
    }

    public List<Measure> getSimilarityMeasures() {
        return similarity_measures;
    }

    public void setSimilarityMeasures(List<Measure> similarity_measures) {
        this.similarity_measures = similarity_measures;
    }

    public void addMeasure(Measure measure) {
        if (similarity_measures.stream().anyMatch(m -> measure.similarity_measure() == m.similarity_measure()))
            throw new MeasureAlreadyExistException();
        similarity_measures.add(measure);
    }

    public double average() {
        return similarity_measures.stream()
                .mapToDouble(Measure::score)
                .average()
                .orElse(0.0);
    }

    public double weightedAverage(boolean join) {
        double result = 0;
        double total_weight = 0;
        for (Measure measure: similarity_measures) {
            double weight = join
                    ? SimilarityMeasures.getJoinWeight(measure.similarity_measure())
                    : SimilarityMeasures.getUnionWeight(measure.similarity_measure());
            result += measure.score() * weight;
            total_weight += weight;
        }
        return result / total_weight;
    }

    @Override
    public int compareTo(Score o) {
        return Double.compare(this.average(), o.average());
    }

    @Override
    public String toString() {
        List<String> measure_strings = similarity_measures.stream().map(Measure::toString).toList();
        return "{Score: " + String.join(", ", measure_strings) + "}";
    }
}
