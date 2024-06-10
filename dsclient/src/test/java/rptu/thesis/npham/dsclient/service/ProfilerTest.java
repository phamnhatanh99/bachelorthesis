package rptu.thesis.npham.dsclient.service;

import org.junit.jupiter.api.Test;
import rptu.thesis.npham.dscommon.model.sketch.Sketch;
import rptu.thesis.npham.dscommon.model.sketch.SketchType;

public class ProfilerTest {

    private final Profiler profiler = new Profiler();

    @Test
    void qGramTest() {
        String s = "Day";
        Sketch sketch = profiler.createNameSketch(s, SketchType.COLUMN_NAME);
        System.out.println(sketch);
    }

}
