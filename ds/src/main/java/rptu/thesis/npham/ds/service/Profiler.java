package rptu.thesis.npham.ds.service;

import lazo.sketch.LazoSketch;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.utils.Constants;
import rptu.thesis.npham.ds.utils.StringUtils;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Service for table's metadata extraction.
 */
@Service
public class Profiler {

    /**
     * Extracts metadata from a table.
     * @param table the table to extract metadata from
     * @return a list of metadata
     */
    public static List<Metadata> profile(Table table) {
        List<Metadata> result = new ArrayList<>();
        String table_name = StringUtils.normalize(table.name());
        String uuid = UUID.randomUUID().toString().replace("-","");
        for (Column<?> column : table.columns()) {
            Metadata metadata = extract(column);
            metadata.setTable_name(table_name);
            metadata.setId(uuid + Constants.SEPARATOR + metadata.getColumn_name());
            result.add(metadata);
        }
        return result;
    }

    private static Metadata extract(Column<?> column) {
        Metadata metadata = new Metadata();
        String column_name = StringUtils.normalize(column.name());
        String placeholder = "";
        String column_type = column.type().name();
        long cardinality = column.size();
        metadata.setCardinality(cardinality);
        LazoSketch sketch = Lazo.createSketch(column);
        metadata.setCardinality(sketch.getCardinality());
        metadata.setSketch(sketch.getHashValues());
        metadata.setColumn_name(column_name);
        metadata.setDescription(placeholder);
        metadata.setType(column_type);
        metadata.setSource(placeholder);

        return metadata;
    }
}
