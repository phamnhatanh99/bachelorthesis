package rptu.thesis.npham.dsclient.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Set;

public class ProfilerTest {

    private final Profiler profiler = new Profiler();

    @Test
    void qGramTest() {
        String s1 = "abc";
        String s2 = "abc";
        Set<String> q1 = profiler.qGram(s1);
        Set<String> q2 = profiler.qGram(s2);
        Assertions.assertEquals(q1, q2);
    }

    @Test
    void formatPatternTest() {
        ArrayList<String> s = new ArrayList<>();
        s.add("abc");
        Set<String> format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("l"));
        s.add("123");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("n"));
        s.add("ABC");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("u"));
        s.add("1Bc");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("a"));
        s.add("a b c");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("lwlwl"));
        s.add("a,b,c");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("lplpl"));
        s.add("Abc");
        format = profiler.generateFormatPatterns(s);
        Assertions.assertTrue(format.contains("c"));
    }

}
