package rptu.thesis.npham.dsclient.Model;

public class QueryForm extends Form {

    private String mode;
    private int limit;
    private double threshold;

    public QueryForm() {
        super();
    }

    public QueryForm(String path) {
        super(path);
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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
