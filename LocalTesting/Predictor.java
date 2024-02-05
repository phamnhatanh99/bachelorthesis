import java.sql.SQLException;
import java.util.*;

public class Predictor {

    private final Analyzer analyzer;
    private final GroundTruth groundTruth;

    public Predictor(String url, String user, String password) throws SQLException {
        analyzer = new Analyzer(url, user, password);
        groundTruth = new GroundTruth();
    }

    /**
     * Return the tables' names as list of string.
     * @return String list containing the tables' names
     */
    public List<String> listTables() {
        return analyzer.listTables();
    }

    /**
     * Return the list of tables as a string.
     * @return listing the tables' names
     */
    public String listTablesAsString() {
        return analyzer.listTablesAsString();
    }

    /**
     * Predicts if the columns in the given table have any matching join partner.
     * @param table_name Name of the table to find join partners.
     */
    public void predict(String table_name) throws SQLException {
        Map<String, Set<String>> map = analyzer.groupColumnsByType(table_name);
        for (String name : map.keySet()) {
            predict(name, map.get(name));
        }
    }

    /**
     * Prints out possible the columns that a given column can join. Columns are possibly joinable when they fulfill at least 2 conditions.
     * @param table_and_column_name Name of the column to predict
     * @param similar_tables_and_columns_names Set of possible join candidates
     */
    private void predict(String table_and_column_name, Set<String> similar_tables_and_columns_names) throws SQLException {
        Set<String> set1 = top3(analyzer.rankColumnByHistogramBounds(table_and_column_name, similar_tables_and_columns_names)).keySet();
        Set<String> set2 = top3(analyzer.rankColumnByAvgWidth(table_and_column_name, similar_tables_and_columns_names)).keySet();
        Set<String> set3 = top3(analyzer.rankColumnByMostCommonFreqs(table_and_column_name, similar_tables_and_columns_names)).keySet();
        Set<String> set4 = top3(analyzer.rankColumnByColumnName(table_and_column_name, similar_tables_and_columns_names)).keySet();
        Map<String, Integer> count = getCount(set1, set2, set3, set4);
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Integer> pair: count.entrySet()) {
            if (pair.getValue() >= 2)
                result.add(pair.getKey());
        }
        String column_name = table_and_column_name.split("_", 2)[1];
        System.out.println("Column " + column_name + " can be joined with: " + result);
        groundTruth.check(column_name);
    }

    /**
     * Filters out a map, keeping only entries with the top 3 lowest/best values.
     * Multiples entries in the same ranking are allowed.
     * @param map The map to filter out the top 3
     * @return Filtered map with only top 3 values
     */
    private Map<String, Double> top3(Map<String, Double> map) {
        int counter = 0;
        double max = - Double.MAX_VALUE;
        Map<String, Double> result = new LinkedHashMap<>();
        for (Map.Entry<String, Double> pair : map.entrySet()) {
            if (pair.getValue() > max)
                counter++;
            if (counter > 3) break; // Stop when top 3 values are found
            result.put(pair.getKey(), pair.getValue());
            max = pair.getValue();
        }
        return result;
    }

    /**
     * Helper function, counts the occurrence of the elements in each set and store the count as value in a map.
     * @return A map from the elements in the sets to its number of appearance
     */
    private static Map<String, Integer> getCount(Set<String> set1, Set<String> set2, Set<String> set3, Set<String> set4) {
        Map<String, Integer> result = new HashMap<>();
        List<Set<String>> sets = new ArrayList<>();
        sets.add(set1);
        sets.add(set2);
        sets.add(set3);
        sets.add(set4);

        for (Set<String> set : sets) {
            for (String s : set) {
                result.put(s, result.getOrDefault(s, 0) + 1); // Increment the count of the element
            }
        }
        return result;
    }
}
