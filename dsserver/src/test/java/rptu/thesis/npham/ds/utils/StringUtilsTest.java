package rptu.thesis.npham.ds.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class StringUtilsTest {

    @Test
    public void testNormalize() {
        String input = "Hello World";
        String expected = "hello world";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeEmpty() {
        String input = "";
        String expected = "";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeSpecialChars() {
        String input = "Hello, World!";
        String expected = "hello, world!";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeNumbers() {
        String input = "1234567890";
        String expected = "1234567890";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeMixed() {
        String input = "Hello, 1234567890!";
        String expected = "hello, 1234567890!";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeMixedSpecialChars() {
        String input = "Hello, 1234567890!@#$%^&*()";
        String expected = "hello, 1234567890!@#$%^&*()";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testGenerateFormatPatternsCapitalized() {
        List<String> column = new ArrayList<>();
        column.add("123 abc");
        Set<String> actual = StringUtils.generateFormatPatterns(column);
        System.out.println(actual);
    }
}
