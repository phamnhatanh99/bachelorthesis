package rptu.thesis.npham.dscommon.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class StringUtilsTest {

    @Test
    public void testNormalize() {
        String input = "Hello World";
        String expected = "hello_world";
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
        String expected = "hello,_world!";
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
        String expected = "hello,_1234567890!";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testNormalizeMixedSpecialChars() {
        String input = "Hello, 1234567890!@#$%^&*()";
        String expected = "hello,_1234567890!@#$%^&*()";
        String actual = StringUtils.normalize(input);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testTokenizeWhitespace() {
        String input = "Hello World  123";
        Set<String> expected = new HashSet<>(Arrays.asList("Hello", "World", "123"));
        Set<String> actual = new HashSet<>(StringUtils.tokenize(input));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testTokenizeUnderscore() {
        String input = "Hello_World__123";
        Set<String> expected = new HashSet<>(Arrays.asList("Hello", "World", "123"));
        Set<String> actual = new HashSet<>(StringUtils.tokenize(input));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testRemoveStopWords() {
        List<String> input = Arrays.asList("Hello", "World", "123", "the", "a", "An", "IS", "aRe", "waS", "were", "be", "been", "being");
        Set<String> expected = new HashSet<>(Arrays.asList("hello", "world", "123"));
        Set<String> actual = new HashSet<>(StringUtils.removeStopWords(input));
        Assertions.assertEquals(expected, actual);
    }
}
