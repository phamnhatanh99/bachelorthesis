package rptu.thesis.npham.dscommon.utils;

public class MethodTimer {

    private long start;
    private final String method_name;

    public MethodTimer() {
        this.method_name = "";
        reset();
    }

    public MethodTimer(String method_name) {
        this.method_name = method_name;
        reset();
    }

    private void reset() {
        start = 0;
    }

    public void start() {
        reset();
        start = System.nanoTime();
    }

    public double getElapsed() {
        return (double) (System.nanoTime() - start) / 1000000;
    }

    public void printElapsed() {
        if (method_name.isEmpty()) return;
        System.out.println("Method " + method_name + " finished in: " + getElapsed() + " ms");
    }
}
