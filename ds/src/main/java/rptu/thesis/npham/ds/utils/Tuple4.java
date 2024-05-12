package rptu.thesis.npham.ds.utils;

public record Tuple4(double first, double second, double third, double fourth) implements Comparable<Tuple4>{
    public double average() {
        return (first + second + third + fourth) / 4;
    }

    public double weightedAverage() {
        double w1 = 1;
        double w2 = 2;
        double w3 = 3;
        double w4 = 2;
        return (w1 * first + w2 * second + w3 * third + w4 * fourth) / (w1 + w2 + w3 + w4);
    }

    @Override
    public int compareTo(Tuple4 o) {
        return Double.compare(this.average(), o.average());
    }

    @Override
    public String toString() {
        return "Tuple4{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                ", fourth=" + fourth +
                '}';
    }

}
