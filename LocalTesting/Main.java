import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;
import lazo.sketch.Sketch;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws SQLException {
//        Instant start = Instant.now();
//        DatabaseObject database_object = new DatabaseObject("jdbc:postgresql://localhost:5432/lookup_prediction", "postgres", "1234");
//        Analyzer analyzer = new Analyzer("jdbc:postgresql://localhost:5432/pagila", "postgres", "1234");
//        Map<Map.Entry<Pair, Pair>, List<Double>> result = analyzer.analyze();
//        for (Map.Entry<Map.Entry<Pair, Pair>, List<Double>> entry : result.entrySet()) {
//            try {
//                database_object.insertPrediction(entry.getKey().getKey(), entry.getKey().getValue(), entry.getValue().get(0), entry.getValue().get(1), entry.getValue().get(2), entry.getValue().get(3), entry.getValue().get(4));
//            } catch (PSQLException e) {
//                System.out.println("Error: " + e.getMessage());
//                System.out.println(entry.getKey().getKey().toString() + " - " + entry.getKey().getValue().toString() + " - " + entry.getValue().toString());
//            }
//        }
//        Instant finish = Instant.now();
//        System.out.println("Prediction table updated. Time elapsed in seconds: " + Duration.between(start, finish).toSeconds());
        test();
    }

    public static void test() throws SQLException {
        Extractor extractor = new Extractor("jdbc:postgresql://localhost:5432/pagila", "postgres", "1234");
        List<Column> extracted = extractor.extractColumns();
        LazoIndex intIndex = new LazoIndex();
        LazoIndex doubleIndex = new LazoIndex();
        LazoIndex stringIndex = new LazoIndex();
        Map<Pair, LazoSketch> sketchMap = new HashMap<>();
        for (Column column : extracted) {
            LazoSketch sketch = new LazoSketch();
            sketch.setHashValues(column.sketch());
            sketch.setCardinality(column.cardinality());
            int type = column.data_type();
            Pair pair = new Pair(column.table_name(), column.column_name());
            if (DatabaseObject.isInteger(type))
                intIndex.insert(pair, sketch);
            if (DatabaseObject.isString(type))
                stringIndex.insert(pair, sketch);
            if (DatabaseObject.isDouble(type))
                doubleIndex.insert(pair, sketch);
            sketchMap.put(pair, sketch);
        }
        Pair target = new Pair("city", "city");
        Pair source = new Pair("address", "district");
        Set<LazoCandidate> candidate = intIndex.query(sketchMap.get(source), 0f, 0f).stream().
                filter(c -> c.key.equals(target)).collect(Collectors.toSet());
        System.out.println(source +  " - " + target + ": " + candidate.iterator().next().js);
    }
}


