package rptu.thesis.npham.dscommon.utils;

public class MethodTimer {

    private long start;
    private long end;
    private long elapsed;
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
        end = 0;
        elapsed = 0;
    }

    public void start() {
        reset();
        start = System.nanoTime();
    }

    public void stop() {
        end = System.nanoTime();
        elapsed = end - start;
        printElapsed();
    }

    public void printElapsed() {
        System.out.println("Method " + method_name + " finished in: " + elapsed / 1000000 + " ms");
    }
}
