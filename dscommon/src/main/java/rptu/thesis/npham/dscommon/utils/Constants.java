package rptu.thesis.npham.dscommon.utils;

import rptu.thesis.npham.dscommon.exceptions.UnreachableException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String SEPARATOR = "__-__";

    public static final List<String> STRINGY_TYPES = new ArrayList<>(Arrays.asList("TEXT", "STRING"));
    public static final List<String> WHOLE_TYPES = new ArrayList<>(Arrays.asList("INTEGER", "BOOLEAN", "LONG", "SHORT"));
    public static final List<String> DECIMAL_TYPES = new ArrayList<>(Arrays.asList("DOUBLE", "FLOAT"));
    public static final List<String> TEMPORAL_TYPES = new ArrayList<>(Arrays.asList("LOCAL_DATE", "LOCAL_DATE_TIME", "LOCAL_TIME"));

    public static List<String> typeInList(String type) {
        if (STRINGY_TYPES.contains(type)) {
            return STRINGY_TYPES;
        } else if (WHOLE_TYPES.contains(type)) {
            return WHOLE_TYPES;
        } else if (DECIMAL_TYPES.contains(type)) {
            return DECIMAL_TYPES;
        } else if (TEMPORAL_TYPES.contains(type)) {
            return TEMPORAL_TYPES;
        } else {
            throw new UnreachableException();
        }
    }
}
