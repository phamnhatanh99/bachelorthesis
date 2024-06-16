package rptu.thesis.npham.dscommon.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class UtilsTest {

    @Test
    public void testTrimCSVSuffix() {
        String file_name = "file.csv";
        String expected = "file";
        String actual = CSV.trimCSVSuffix(file_name);
        Assertions.assertEquals(expected, actual);

        file_name = "file";
        expected = "file";
        actual = CSV.trimCSVSuffix(file_name);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testTypeInList() {
        String type = "TEXT";
        List<String> expected = Constants.STRINGY_TYPES;
        List<String> actual = Constants.typeInList(type);
        Assertions.assertEquals(expected, actual);

        type = "INTEGER";
        expected = Constants.WHOLE_TYPES;
        actual = Constants.typeInList(type);
        Assertions.assertEquals(expected, actual);

        type = "DOUBLE";
        expected = Constants.DECIMAL_TYPES;
        actual = Constants.typeInList(type);
        Assertions.assertEquals(expected, actual);

        type = "LOCAL_DATE";
        expected = Constants.TEMPORAL_TYPES;
        actual = Constants.typeInList(type);
        Assertions.assertEquals(expected, actual);
    }
}
