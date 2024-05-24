package rptu.thesis.npham.ds.service;

import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.metadata.Metadata;
import rptu.thesis.npham.ds.model.sketch.Sketch;
import rptu.thesis.npham.ds.model.sketch.Sketches;
import rptu.thesis.npham.ds.service.lazo.Lazo;
import rptu.thesis.npham.ds.service.lazo.SketchType;
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
    public List<Pair<Metadata, Sketches>> profile(Table table, String address) {
        List<Pair<Metadata, Sketches>> result = new ArrayList<>();
        String table_name = StringUtils.normalize(table.name());
        String uuid = UUID.randomUUID().toString().replace("-","");
        int arity = table.columnCount();
        for (Column<?> column : table.columns()) {
            String column_name = StringUtils.normalize(column.name());
            String id = uuid + StringUtils.SEPARATOR + column_name;
            String column_type = column.type().name();
            int size = column.size();

            Metadata metadata = createMetadata(id, table_name, column_name, column_type, size, arity, address);
            Sketches sketches = createSketches(id, column, table_name, column_name);

            result.add(new Pair<>(metadata, sketches));
        }
        return result;
    }

    private Metadata createMetadata(String id, String table_name, String column_name, String type, int size, int arity, String address) {
        Metadata metadata = new Metadata();
        metadata.setId(id);
        metadata.setTableName(table_name);
        metadata.setColumnName(column_name);
        metadata.setType(type);
        metadata.setSize(size);
        metadata.setArity(arity);
        metadata.setAddress(address);
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
        int k = 4;
        Set<String> shingles = StringUtils.shingle(name, k);
        LazoSketch lazo_name_sketch = lazo.createSketch(shingles);
        return new Sketch(type, lazo_name_sketch.getCardinality(), lazo_name_sketch.getHashValues());
    }

    private Sketch createColumnSketch(Column<?> column) {
        LazoSketch lazo_column_sketch = lazo.createSketch(column);
        SketchType column_sketch_type;
        column_sketch_type = SketchType.COLUMN_VALUE;
        return new Sketch(column_sketch_type, lazo_column_sketch.getCardinality(), lazo_column_sketch.getHashValues());
    }

    private Sketch createFormatSketch(Column<?> column) {
        Set<String> format_patterns = StringUtils.generateFormatPatterns(column.asStringColumn().asSet());
        LazoSketch lazo_format_sketch = lazo.createSketch(format_patterns);
        return new Sketch(SketchType.FORMAT, lazo_format_sketch.getCardinality(), lazo_format_sketch.getHashValues());
    }
}
