package rptu.thesis.npham.dsclient.Model;

public class QueryForm extends Form {
    private int limit;
    private double threshold;

    public QueryForm() {
        super();
    }

    public QueryForm(String path, int limit, double threshold) {
        super(path);
        this.limit = limit;
        this.threshold = threshold;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }
}
