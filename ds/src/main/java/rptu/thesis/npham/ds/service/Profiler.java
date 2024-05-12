package rptu.thesis.npham.ds.service;

import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.model.Sketch;
import rptu.thesis.npham.ds.model.Sketches;
import rptu.thesis.npham.ds.utils.Constants;
import rptu.thesis.npham.ds.utils.Pair;
import rptu.thesis.npham.ds.utils.StringUtils;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.*;

/**
 * Service for table's metadata extraction.
 */
@Service
public class Profiler {

    private final Lazo lazo;

    @Autowired
    public Profiler(Lazo lazo) {
        this.lazo = lazo;
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
        for (Column<?> column : table.columns()) {
            String column_name = StringUtils.normalize(column.name());
            String id = uuid + Constants.SEPARATOR + column_name;
            String column_type = column.type().name();
            String placeholder = ""; //TODO: change this

            Metadata metadata = createMetadata(id, table_name, column_name, column_type, placeholder, placeholder);
            Sketches sketches = createSketches(id, column, column_type);

            result.add(new Pair<>(metadata, sketches));
        }
        return result;
    }

    private Metadata createMetadata(String id, String table_name, String column_name, String type, String description, String uri) {
        Metadata metadata = new Metadata();
        metadata.setId(id);
        metadata.setTableName(table_name);
        metadata.setColumnName(column_name);
        metadata.setType(type);
        metadata.setDescription(description);
        metadata.setUri(uri);
        return metadata;
    }

    // TODO: maybe optimize to read the column only once and create all sketches at once
    private Sketches createSketches(String id, Column<?> column, String type) {
        LazoSketch lazo_column_sketch = lazo.createSketch(column);
        String column_sketch_type = Constants.STRINGY_TYPES.contains(type) ? Constants.STRING_SKETCH : Constants.NUMERIC_SKETCH;
        Sketch column_sketch = new Sketch(column_sketch_type, lazo_column_sketch.getCardinality(), lazo_column_sketch.getHashValues());

        Set<String> format_patterns = StringUtils.generateFormatPatterns(column.asStringColumn().asSet());
        LazoSketch lazo_format_sketch = lazo.createSketch(format_patterns);
        Sketch format_sketch = new Sketch(Constants.FORMAT_SKETCH, lazo_format_sketch.getCardinality(), lazo_format_sketch.getHashValues());

        Sketches sketches = new Sketches();
        sketches.setId(id);
        sketches.setSketches(new HashSet<>(Arrays.asList(column_sketch, format_sketch)));

        return sketches;
    }
}
