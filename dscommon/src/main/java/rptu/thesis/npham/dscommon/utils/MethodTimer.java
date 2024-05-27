package rptu.thesis.npham.dscommon.utils;

public class MethodTimer {

    private long start;
    private long end;
    private long elapsed;
    private String method_name;

    public MethodTimer() {
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
        System.out.println("Method: " + method_name);
        start = System.nanoTime();
    }

    public void end() {
        end = System.nanoTime();
        elapsed = end - start;
    }

    public void printElapsed() {
        System.out.println("Elapsed time: " + elapsed / 1000000 + " ms");
    }
}
