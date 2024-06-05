package rptu.thesis.npham.dsclient.service;

import lazo.sketch.LazoSketch;
import lazo.sketch.SketchType;
import org.springframework.stereotype.Service;

@Service
public class SketchGenerator {
    private static final int N_PERMUTATIONS = 64;

    /**
     * Creates a new MinHash sketch from an iterable (e.g. a column).
     */
    public LazoSketch createSketch(Iterable<?> iterable) {
        LazoSketch sketch = createEmptySketch();
        return updateSketch(iterable, sketch);
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
            else sketch.update(value.toString().trim());
        }
        return sketch;
    }
}
