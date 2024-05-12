package rptu.thesis.npham.ds.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String SEPARATOR = "__.__";
    public static final List<String> STRINGY_TYPES = new ArrayList<>(Arrays.asList("TEXT", "STRING", "LOCAL_DATE", "LOCAL_DATE_TIME", "LOCAL_TIME"));
    public static final List<String> NUMERIC_TYPES = new ArrayList<>(Arrays.asList("INTEGER", "BOOLEAN", "DOUBLE", "LONG", "FLOAT", "SHORT"));

    public static final String STRING_SKETCH = "string";
    public static final String NUMERIC_SKETCH = "numeric";
    public static final String FREQUENT_SKETCH = "frequent";
    public static final String FORMAT_SKETCH = "format";

}
