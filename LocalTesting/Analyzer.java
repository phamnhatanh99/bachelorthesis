import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Analyzer {
    private final DatabaseObject database_object;
    private final FeatureCompare compare;

    public Analyzer(String url, String user, String password) throws SQLException {
        database_object = new DatabaseObject(url, user, password);
        compare = new FeatureCompare(url, user, password);
    }

    public List<Map.Entry<Pair, Pair>> createPairs() throws SQLException {
        List<Map.Entry<Pair, Pair>> pairs = new ArrayList<>();
        List<String> tables = database_object.listTables();
        for (String source_table : tables) {
            if (source_table.equals("lookup_prediction")) continue;
            for (String target_table : tables) {
                if (source_table.equals(target_table) || target_table.equals("lookup_prediction")) continue;
                ResultSet source_columns = database_object.getColumns(source_table);
                ResultSet target_columns = database_object.getColumns(target_table);
                while (source_columns.next()) {
                    String source_column = source_columns.getString(4);
                    while (target_columns.next()) {
                        String target_column = target_columns.getString(4);
                        Pair source = new Pair(source_table, source_column);
                        Pair target = new Pair(target_table, target_column);
                        if (pairs.contains(new SimpleEntry<>(target, source))) continue;
                        pairs.add(new SimpleEntry<>(source, target));
                    }
                    target_columns.beforeFirst();
                }
            }
        }
        return pairs;
    }

    public Map<Map.Entry<Pair, Pair>, List<Double>> analyze() throws SQLException {
        Map<Map.Entry<Pair, Pair>, List<Double>> result = new HashMap<>();
        List<Map.Entry<Pair, Pair>> pairs_list = createPairs();
        for (Map.Entry<Pair, Pair> pair: pairs_list) {
            Pair source = pair.getKey();
            Pair target = pair.getValue();
            double table_name = compare.tableName(source, target);
            double column_name = compare.attributeName(source, target);
            double avg_width = compare.avgWidth(source, target);
            double most_common_vals = compare.mostCommonVals(source, target);
            double jaccard_estimate = compare.jaccardSimilarity(source, target);
            result.put(new SimpleEntry<>(source, target), List.of(table_name, column_name, avg_width, most_common_vals, jaccard_estimate));
            result.put(new SimpleEntry<>(target, source), List.of(table_name, column_name, avg_width, most_common_vals, jaccard_estimate));
        }
        return result;
    }
}
