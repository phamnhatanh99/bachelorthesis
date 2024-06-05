package rptu.thesis.npham.dsserver.model.similarity;

import java.util.List;

public class Measures implements Comparable<Measures> {

    private final List<Measure> measures;

    public Measures(List<Measure> measures) {
        this.measures = measures;
    }

    public void addMeasure(Measure measure) {
        if (measures.stream().anyMatch(m -> measure.measures() == m.measures()))
            throw new RuntimeException("Measure already exist in the list");
        measures.add(measure);
    }

    public void clear() {
        measures.clear();
    }

    public boolean isEmpty() {
        return measures.isEmpty();
    }

    public int totalWeight() {
        return measures.stream()
                .mapToInt(Measure::weight)
                .sum();
    }

    public double weightedSum() {
        return measures.stream()
                .mapToDouble(m -> m.score() * m.weight())
                .sum();
    }

    public double average() {
        return measures.stream()
                .mapToDouble(Measure::score)
                .average()
                .orElse(0.0);
    }

    public double weightedAverage() {
        double result = 0;
        double total_weight = 0;
        for (Measure measure: measures) {
            double weight = measure.weight();
            result += measure.score() * weight;
            total_weight += weight;
        }
        return result / total_weight;
    }

    @Override
    public int compareTo(Measures o) {
        return Double.compare(this.average(), o.average());
    }

    @Override
    public String toString() {
        List<String> measure_strings = measures.stream().map(Measure::toString).toList();
        return "{Measures: " + String.join(", ", measure_strings) + "}";
    }
}
