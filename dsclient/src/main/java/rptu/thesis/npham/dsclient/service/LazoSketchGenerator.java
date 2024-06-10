package rptu.thesis.npham.dsclient.service;

import lazo.sketch.LazoSketch;
import lazo.sketch.SketchType;

public class LazoSketchGenerator {
    private static final int N_PERMUTATIONS = 128;

    /**
     * Creates a new MinHash sketch from an iterable (e.g. a column).
     */
    public LazoSketch createSketch(Iterable<?> iterable) {
        return updateSketch(iterable, createEmptySketch());
    }

    public LazoSketch createEmptySketch() {
        return new LazoSketch(N_PERMUTATIONS, SketchType.MINHASH_OPTIMAL);
    }

    public LazoSketch updateSketch(Iterable<?> iterable, LazoSketch sketch) {
        // If the iterable is empty
        if (!iterable.iterator().hasNext()) {
            sketch.update("");
            return sketch;
        }
        for (Object value: iterable) {
            if (value == null) sketch.update("");
            else if (value instanceof Boolean b) sketch.update(b ? "1" : "0");
            else sketch.update(value.toString().trim());
        }
        return sketch;
    }
}
