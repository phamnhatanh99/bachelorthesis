package rptu.thesis.npham.dsclient.service;

import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.dscommon.model.dto.Summaries;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.sketch.Sketch;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dscommon.model.sketch.SketchType;
import rptu.thesis.npham.dscommon.utils.MethodTimer;
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

    private static final int K = 4;

    private final SketchGenerator sketch_generator;

    @Autowired
    public Profiler(SketchGenerator sketch_generator) {
        this.sketch_generator = sketch_generator;
    }

    /**
     * Summarize a table, creating metadata and sketches for each column.
     * @param table the table to profile from
     * @return a list of metadata and sketches for each column
     */
    public List<Summaries> profile(Table table) {
        MethodTimer timer1 = new MethodTimer("Profile table: " + table.name());
        timer1.start();
        List<Summaries> result = new ArrayList<>();
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
            MethodTimer timer = new MethodTimer(table_name + Constants.SEPARATOR + column_name);
            timer.start();
            Sketches sketches = createSketches(id, column, table_name, column_name);
            timer.stop();

            result.add(new Summaries(metadata, sketches));
        }
        timer1.stop();
        return result;
    }

    public Metadata createMetadata(String id, String table_name, String column_name, String type, int size, int arity, Set<String> address) {
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
    public Sketches createSketches(String id, Column<?> column, String table_name, String column_name) {
        Sketch table_name_sketch = createNameSketch(table_name, SketchType.TABLE_NAME);
        Sketch column_name_sketch = createNameSketch(column_name, SketchType.COLUMN_NAME);
        Sketch column_sketch = createColumnSketch(column);
        Sketch format_sketch = createFormatSketch(column);

        Sketches sketches = new Sketches();
        sketches.setId(id);
        sketches.setSketches(new HashSet<>(Arrays.asList(table_name_sketch, column_name_sketch, column_sketch, format_sketch)));

        return sketches;
    }

    public Sketch createNameSketch(String name, SketchType type) {
        Set<String> shingles = shingle(name);
        LazoSketch lazo_name_sketch = sketch_generator.createSketch(shingles);
        return new Sketch(type, lazo_name_sketch.getCardinality(), lazo_name_sketch.getHashValues());
    }

    public Sketch createColumnSketch(Column<?> column) {
//        LazoSketch lazo_column_sketch = sketch_generator.createSketch(column.asSet());
        LazoSketch lazo_column_sketch = sketch_generator.createEmptySketch();
        if (Constants.typeInList(column.type().name()).equals(Constants.STRINGY_TYPES)) {
            for (String s: column.asStringColumn().asSet()) {
                Set<String> shingles = shingle(s);
                lazo_column_sketch = sketch_generator.updateSketch(shingles, lazo_column_sketch);
            }
        }
        else {
            lazo_column_sketch = sketch_generator.updateSketch(column, lazo_column_sketch);
        }
        return new Sketch(SketchType.COLUMN_VALUE, lazo_column_sketch.getCardinality(), lazo_column_sketch.getHashValues());
    }

    public Sketch createFormatSketch(Column<?> column) {
        Set<String> format_patterns = generateFormatPatterns(column.asStringColumn().asSet());
        LazoSketch lazo_format_sketch = sketch_generator.createSketch(format_patterns);
        return new Sketch(SketchType.FORMAT, lazo_format_sketch.getCardinality(), lazo_format_sketch.getHashValues());
    }

    public Set<String> shingle(String s) {
        Set<String> res = new HashSet<>();
        for (int i = 0; i < s.length() - K + 1; i++) {
            res.add(s.substring(i, i + K));
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
