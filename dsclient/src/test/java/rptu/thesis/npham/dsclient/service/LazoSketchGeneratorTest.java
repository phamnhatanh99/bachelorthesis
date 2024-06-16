package rptu.thesis.npham.dsclient.service;

import lazo.sketch.LazoSketch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class LazoSketchGeneratorTest {

    LazoSketchGenerator generator = new LazoSketchGenerator();

    @Test
    void emptySketchTest() {
        LazoSketch sketch = generator.createEmptySketch();
        Assertions.assertEquals(0, sketch.getCardinality());
    }

    @Test
    void updateSketchTest() {
        LazoSketch sketch = generator.createEmptySketch();
        sketch = generator.updateSketch(new HashSet<>(), sketch);
        LazoSketch sketch2 = generator.updateSketch(List.of(""), generator.createEmptySketch());
        Assertions.assertEquals(1, sketch.getCardinality());
        Set<Long> hashValues = new HashSet<>();
        for (int i = 0; i < sketch.getHashValues().length; i++) {
            hashValues.add(sketch.getHashValues()[i]);
        }
        Set<Long> hashValues2 = new HashSet<>();
        for (int i = 0; i < sketch2.getHashValues().length; i++) {
            hashValues2.add(sketch2.getHashValues()[i]);
        }
        Assertions.assertEquals(hashValues, hashValues2);

    }
}
