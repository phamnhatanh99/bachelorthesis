package rptu.thesis.npham.ds.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final List<String> STRINGY_TYPES = new ArrayList<>(Arrays.asList("TEXT", "STRING"));
    public static final List<String> NUMERIC_TYPES = new ArrayList<>(Arrays.asList("INTEGER", "BOOLEAN", "DOUBLE", "LONG", "FLOAT", "SHORT"));
    public static final List<String> TEMPORAL_TYPES = new ArrayList<>(Arrays.asList("LOCAL_DATE", "LOCAL_DATE_TIME", "LOCAL_TIME"));
}
