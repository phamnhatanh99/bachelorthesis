package rptu.thesis.npham.dsclient.service.lazo;

import lazo.sketch.LazoSketch;
import org.springframework.stereotype.Service;

@Service
public class SketchGenerator {
    private static final int N_PERMUTATIONS = 256;

    /**
     * Creates a new MinHash sketch from an iterable (e.g. a column).
     */
    public LazoSketch createSketch(Iterable<?> iterable) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS);
        for (Object value: iterable)
            if (value == null) sketch.update("");
            else sketch.update(value.toString().trim());
        return sketch;
    }
}
