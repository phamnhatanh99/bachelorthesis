package rptu.thesis.npham.dsclient.service;

import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.sketch.Sketch;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dscommon.model.sketch.SketchType;
import rptu.thesis.npham.dscommon.utils.MethodTimer;
import rptu.thesis.npham.dscommon.utils.Pair;
import rptu.thesis.npham.dscommon.utils.StringUtils;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for table's metadata extraction.
 */
@Service
public class Profiler {

    private static final Pattern ALPHANUMERIC = Pattern.compile("^(?:[0-9]+[a-zA-Z]|[a-zA-Z]+[0-9])[a-zA-Z0-9]*");
    private static final Pattern CAPITALIZED = Pattern.compile("^[A-Z][a-z]+");
    private static final Pattern UPPERCASE = Pattern.compile("^[A-Z]+");
    private static final Pattern LOWERCASE = Pattern.compile("^[a-z]+");
    private static final Pattern NUMBER = Pattern.compile("^[0-9]+");
    private static final Pattern PUNCTUATION = Pattern.compile("^\\p{Punct}+");
    private static final Pattern WHITESPACE = Pattern.compile("^\\s+");
    private static final Pattern OTHER = Pattern.compile(".");

    private final SketchGenerator sketch_generator;

    @Autowired
    public Profiler(SketchGenerator sketch_generator) {
        this.sketch_generator = sketch_generator;
    }

    /**
     * Extracts metadata from a table.
     * @param table the table to extract metadata from
     * @return a list of metadata
     */
    public List<Pair<Metadata, Sketches>> profile(Table table) {
        List<Pair<Metadata, Sketches>> result = new ArrayList<>();
        String table_name = StringUtils.normalize(table.name());
        String uuid = UUID.randomUUID().toString().replace("-","");
        int arity = table.columnCount();
        Set<String> address = new HashSet<>();
        try {
            address.add(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        for (Column<?> column : table.columns()) {
            String column_name = StringUtils.normalize(column.name());
            String id = uuid + Constants.SEPARATOR + column_name;
            String column_type = column.type().name();
            int size = column.size();

            Metadata metadata = createMetadata(id, table_name, column_name, column_type, size, arity, address);
            Sketches sketches = createSketches(id, column, table_name, column_name);

            result.add(new Pair<>(metadata, sketches));
        }
        return result;
    }

    private Metadata createMetadata(String id, String table_name, String column_name, String type, int size, int arity, Set<String> address) {
        Metadata metadata = new Metadata();
        metadata.setId(id);
        metadata.setTableName(table_name);
        metadata.setColumnName(column_name);
        metadata.setType(type);
        metadata.setSize(size);
        metadata.setArity(arity);
        metadata.setAddresses(address);
        return metadata;
    }

    // TODO: maybe optimize to read the column only once and create all sketches at once
    /**
     * Creates sketches for a column.
     */
    private Sketches createSketches(String id, Column<?> column, String table_name, String column_name) {
        Sketch table_name_sketch = createNameSketch(table_name, SketchType.TABLE_NAME);
        Sketch column_name_sketch = createNameSketch(column_name, SketchType.COLUMN_NAME);
        Sketch column_sketch = createColumnSketch(column);
        Sketch format_sketch = createFormatSketch(column);

        Sketches sketches = new Sketches();
        sketches.setId(id);
        sketches.setSketches(new HashSet<>(Arrays.asList(table_name_sketch, column_name_sketch, column_sketch, format_sketch)));

        return sketches;
    }

    private Sketch createNameSketch(String name, SketchType type) {
        MethodTimer timer = new MethodTimer("NameSketch");
        timer.start();
        int k = 4;
        Set<String> shingles = shingle(name, k);
        LazoSketch lazo_name_sketch = sketch_generator.createSketch(shingles);
        return new Sketch(type, lazo_name_sketch.getCardinality(), lazo_name_sketch.getHashValues());
    }

    private Sketch createColumnSketch(Column<?> column) {
        MethodTimer timer = new MethodTimer("ColumnSketch");
        timer.start();
        LazoSketch lazo_column_sketch = sketch_generator.createSketch(column);
        SketchType column_sketch_type;
        column_sketch_type = SketchType.COLUMN_VALUE;
        timer.stop();
        return new Sketch(column_sketch_type, lazo_column_sketch.getCardinality(), lazo_column_sketch.getHashValues());
    }

    private Sketch createFormatSketch(Column<?> column) {
        MethodTimer timer = new MethodTimer("FormatSketch");
        timer.start();
        Set<String> format_patterns = generateFormatPatterns(column.asStringColumn().asSet());
        LazoSketch lazo_format_sketch = sketch_generator.createSketch(format_patterns);
        timer.stop();
        return new Sketch(SketchType.FORMAT, lazo_format_sketch.getCardinality(), lazo_format_sketch.getHashValues());
    }

    private Set<String> shingle(String s, int k) {
        if (k < 1) k = 4;
        Set<String> res = new HashSet<>();
        for (int i = 0; i < s.length() - k + 1; i++) {
            res.add(s.substring(i, i + k));
        }
        return res;
    }

    public static Set<String> generateFormatPatterns(Iterable<String> column) {
        Set<String> result = new HashSet<>();
        for (String value : column) {
            if (value != null && !value.trim().isEmpty()) {
                String tokenizedValue = fdTokenize(value.replaceAll("\n", " ").trim());
                result.add(getRegExString(tokenizedValue));
            }
        }
        return result;
    }

    /**
     * Adapted from <a href="https://doi.org/10.1109/ICDE48307.2020.00067">Dataset discovery in data lakes</a>
     */
    private static String fdTokenize(String str) {
        StringBuilder result = new StringBuilder();
        while (!str.isEmpty()) {
            Matcher a = ALPHANUMERIC.matcher(str);
            Matcher c = CAPITALIZED.matcher(str);
            Matcher u = UPPERCASE.matcher(str);
            Matcher l = LOWERCASE.matcher(str);
            Matcher n = NUMBER.matcher(str);
            Matcher p = PUNCTUATION.matcher(str);
            Matcher w = WHITESPACE.matcher(str);
            Matcher o = OTHER.matcher(str);

            if (a.find()) {
                result.append("a");
                str = str.substring(a.group().length());
            } else if (c.find()) {
                result.append("c");
                str = str.substring(c.group().length());
            } else if (u.find()) {
                result.append("u");
                str = str.substring(u.group().length());
            } else if (l.find()) {
                result.append("l");
                str = str.substring(l.group().length());
            } else if (n.find()) {
                result.append("n");
                str = str.substring(n.group().length());
            } else if (p.find()) {
                result.append("p");
                str = str.substring(p.group().length());
            } else if (w.find()) {
                result.append("w");
                str = str.substring(w.group().length());
            } else if (o.find()) {
                result.append("o");
                str = str.substring(o.group().length());
            } else {
                break;
            }
        }
        return result.toString();
    }

    private static String getRegExString(String str) {
        if (str == null || str.isEmpty()) return str;

        StringBuilder result = new StringBuilder();

        int length = str.length();

        for (int i = 0; i < length; i++) {
            result.append(str.charAt(i));
            int count = 1;
            while (i + 1 < length && str.charAt(i) == str.charAt(i + 1)) {
                count++;
                i++;
            }
            if (count > 1) {
                result.append('+');
            }
        }

        return result.toString();
    }
}
