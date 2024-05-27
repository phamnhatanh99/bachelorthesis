package rptu.thesis.npham.dscommon.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


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
}
