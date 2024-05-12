package rptu.thesis.npham.ds.model;

public record Score(double table_name, double column_name, double column_values, double column_format, double frequent_values) implements Comparable<Score> {

    public double average() {
        return (table_name + column_name + column_values + column_format + frequent_values) / 5;
    }

    public double weightedAverage() {
        double w1 = 1;
        double w2 = 2;
        double w3 = 3;
        double w4 = 2;
        double w5 = 2;
        return (w1 * table_name + w2 * column_name + w3 * column_values + w4 * column_format + w5 * frequent_values) / (w1 + w2 + w3 + w4 + w5);
    }

    @Override
    public int compareTo(Score o) {
        return Double.compare(this.average(), o.average());
    }

    @Override
    public String toString() {
        return "Score{" +
                "table_name=" + table_name +
                ", column_name=" + column_name +
                ", column_values=" + column_values +
                ", column_format=" + column_format +
                ", frequent_values=" + frequent_values +
                '}';
    }
}
