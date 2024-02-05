import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A class that contains the methods to perform high-level analysis of the database.
 */
public class Analyzer {

    private final DatabaseObject database_object;

    /**
     * Constructor for Analyzer.
     * @param url The url of the database
     * @param user The username of the database
     * @param password The password of the database
     */
    public Analyzer(String url, String user, String password) throws SQLException {
        database_object = new DatabaseObject(url, user, password);
    }

    /**
     * Return the tables' names as list of string.
     * @return String list containing the tables' names
     */
    public List<String> listTables() {
        return database_object.listTables();
    }

    /**
     * Return the list of tables as a string.
     * @return listing the tables' names
     */
    public String listTablesAsString() {
        return database_object.listTablesAsString();
    }

    /**
     * Returns all other columns that has the same datatype as the columns in the given table.
     * @param table_name Name of the table
     * @return Mapping from each column in the input table to other columns with the same data type. Columns are prepended with the table name.
     */
    public Map<String, Set<String>> groupColumnsByType(String table_name) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        ResultSet input_columns = database_object.getColumns(table_name); // Get the columns of the input table
        while (input_columns.next()) { // Iterate through the columns in the input table
            String input_column_name = input_columns.getString(4);
            String input_column_type = input_columns.getString(6);
            Set<String> similar_columns = new HashSet<>(); // Set of columns with the same data type
            result.put(table_name + "_" + input_column_name, similar_columns);
            for (String table : database_object.listTables()) { // Iterate through all tables
                ResultSet columns = database_object.getColumns(table);
                while (columns.next()) { // Iterate through the columns in the current table
                    String table_name_of_column = columns.getString(3);
                    if (table_name_of_column.equals(table_name)) // Skip the input table
                        continue;
                    String column_name = columns.getString(4);
                    String column_type = columns.getString(6);
                    if (input_column_type.equals(column_type)) // If the column type matches the input column type
                        similar_columns.add(table_name_of_column + "_" + column_name); // Add the column to the similar columns
                }
            }
        }
        return result;
    }

    /**
     * Sort a map by its values.
     * @param map The map to be sorted
     * @return A LinkedHashMap sorted by values
     */
    private Map<String, Double> sortMapByValue(Map<String, Double> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * Ranks the similarity of a column with similar columns according to the range of their values.
     * Similarity is calculated by distance of the distance between lower and upper bounds.
     * Only ranks columns of numeric types, returns empty map for other types.
     * @param table_and_column_name Name of the column (prepended with the table name)
     * @param similar_table_and_column_names Name of the columns to rank (prepended with the table name)
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByHistogramBounds(String table_and_column_name, Set<String> similar_table_and_column_names) throws SQLException {
        Map<String, Double> result = new HashMap<>();
        if (!DatabaseObject.isNumeric(database_object.getColumnDataType(table_and_column_name))) return result; // Return empty map if the column is not numeric
        ResultSet pg_stats = database_object.queryPgStats(); // Get the pg_stats table
        double input_bounds_distance = 0;
        String column_name = table_and_column_name.split("_", 2)[1]; // Get the column name from the input table and column name
        // Calculate bound distance of the input column
        while (pg_stats.next()) { // Iterate through the rows of the pg_stats table to find the input column
            String attname = pg_stats.getString("attname");
            if (attname.equals(column_name)) {
                try {
                    // Get the histogram bounds of the input column
                    Array input_bounds = pg_stats.getArray("histogram_bounds");
                    String temp = input_bounds.toString().replaceAll("[{}]", "");
                    String[] input_bounds_s_list = temp.split(",");
                    List<Float> input_bounds_list = Arrays.stream(input_bounds_s_list).map(Float::parseFloat).toList();
                    // Get the lower and upper bounds of the histogram bounds
                    float lower = input_bounds_list.get(0);
                    float upper = input_bounds_list.get(input_bounds_list.size() - 1);
                    input_bounds_distance = upper - lower;
                } catch (NullPointerException exception) {
                    return result; // Return empty map if the column does not have histogram bounds
                }
                break;
            }
        }
        // Calculate how similar the input column's bound distance is to other similar columns bound distance
        for (String similar_table_and_column_name : similar_table_and_column_names) {
            pg_stats.first();
            pg_stats.previous(); // Move the cursor to the first row
            String similar_column_name = similar_table_and_column_name.split("_", 2)[1]; // Get the column name from the table and column name
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_column_name)) {
                    try {
                        // Do the same thing as above but for the similar column
                        Array bounds = pg_stats.getArray("histogram_bounds");
                        String temp = bounds.toString().replaceAll("[{}]", "");
                        String[] input_bounds_s_list = temp.split(",");
                        List<Float> bounds_list = Arrays.stream(input_bounds_s_list).map(Float::parseFloat).toList();
                        float lower = bounds_list.get(0);
                        float upper = bounds_list.get(bounds_list.size() - 1);
                        double bounds_distance = upper - lower;
                        double similarity = Math.abs(input_bounds_distance - bounds_distance);
                        result.put(similar_column_name, similarity);
                    } catch (NullPointerException ignored) {}
                }
            }
        }
        return sortMapByValue(result); // Return the sorted result
    }

    /**
     * Ranks the similarity of a column with similar columns according to avg_width.
     * Similarity is calculated by distance the 2 avg_widths.
     * @param table_and_column_name Name of the column (prepended with the table name)
     * @param similar_table_and_column_names Name of the columns to rank (prepended with the table name)
     * @return Mapping from similarity values to the column names
     */
    public Map<String, Double> rankColumnByAvgWidth(String table_and_column_name, Set<String> similar_table_and_column_names) throws SQLException {
        Map<String, Double> result = new HashMap<>();
        ResultSet pg_stats = database_object.queryPgStats();
        int input_avg_width = 0;
        String column_name = table_and_column_name.split("_", 2)[1];
        // Find the avg_width of the input column
        while (pg_stats.next()) {
            String attname = pg_stats.getString("attname");
            if (attname.equals(column_name)) {
                input_avg_width = pg_stats.getInt("avg_width");
                break;
            }
        }
        // Calculate the avg_width similarity of the input column with other similar columns
        for (String similar_name : similar_table_and_column_names) {
            pg_stats.first();
            pg_stats.previous();
            String similar_column_name = similar_name.split("_", 2)[1];
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_column_name)) {
                    int avg_width = pg_stats.getInt("avg_width");
                    double similarity = Math.abs(input_avg_width - avg_width);
                    result.put(similar_column_name, similarity);
                }
            }
        }
        return sortMapByValue(result);
    }

    /**
     * Ranks the similarity of a column with similar columns according to most_common_freqs.
     * Similarity is calculated by the Jaccard coefficient between the two most_common_freqs.
     * @param table_and_column_name Name of the column (prepended with the table name)
     * @param similar_table_and_column_names Name of the columns to rank (prepended with the table name)
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByMostCommonFreqs(String table_and_column_name, Set<String> similar_table_and_column_names) throws SQLException {
        Map<String, Double> result = new HashMap<>();
        ResultSet pg_stats = database_object.queryPgStats();
        Set<Float> input_most_common_freqs_set = new HashSet<>();
        String column_name = table_and_column_name.split("_", 2)[1];
        // Find the most_common_freqs of the input column
        while (pg_stats.next()) {
            String attname = pg_stats.getString("attname");
            if (attname.equals(column_name)) {
                try {
                    Array input_most_common_freqs = pg_stats.getArray("most_common_freqs");
                    input_most_common_freqs_set.addAll(List.of((Float[]) input_most_common_freqs.getArray()));
                }
                catch (NullPointerException exception) {
                    return result; // Return empty map if the column does not have most_common_freqs values
                }
                break;
            }
        }
        // Calculate the Jaccard coefficient between the most_common_freqs of the input column and other similar columns
        for (String similar_table_and_column_name : similar_table_and_column_names) {
            pg_stats.first();
            pg_stats.previous();
            String similar_column_name = similar_table_and_column_name.split("_", 2)[1];
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_column_name)) {
                    try {
                        Array most_common_freqs = pg_stats.getArray("most_common_freqs");
                        Set<Float> most_common_freqs_set = new HashSet<>(List.of((Float[]) most_common_freqs.getArray()));
                        // Calculate the Jaccard coefficient
                        Set<Float> intersection = new HashSet<>(input_most_common_freqs_set);
                        intersection.retainAll(most_common_freqs_set);
                        Set<Float> union = new HashSet<>(input_most_common_freqs_set);
                        union.addAll(most_common_freqs_set);
                        if (!union.isEmpty()) {
                            double similarity = ((double) intersection.size()) / union.size();
                            if (similarity != 0)
                                result.put(similar_column_name, - similarity);
                        }
                    }
                    catch (NullPointerException ignored) {}
                }
            }
        }
        return sortMapByValue(result);
    }

    /**
     * Ranks the similarity of a column with similar columns according to the column name.
     * Similarity is calculated by the Jaccard coefficient between the two column names.
     * @param table_and_column_name Name of the column (prepended with the table name)
     * @param similar_table_and_column_names Name of the columns to rank (prepended with the table name)
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByColumnName(String table_and_column_name, Set<String> similar_table_and_column_names) {
        Map<String, Double> result = new HashMap<>();
        String column_name = table_and_column_name.split("_", 2)[1]; // Get the column name from the input table and column name
        for (String similar_table_and_column_name : similar_table_and_column_names) {
            String similar_column_name = similar_table_and_column_name.split("_", 2)[1];
            Set<Character> column_name_set = column_name.chars().mapToObj(c -> (char) c).collect(Collectors.toSet());
            Set<Character> similar_column_name_set = similar_column_name.chars().mapToObj(c -> (char) c).collect(Collectors.toSet());
            Set<Character> intersection = new HashSet<>(column_name_set);
            intersection.retainAll(similar_column_name_set);
            Set<Character> union = new HashSet<>(column_name_set);
            union.addAll(similar_column_name_set);
            if (!union.isEmpty()) {
                double similarity = ((double) intersection.size()) / union.size();
                if (similarity != 0)
                    result.put(similar_column_name, - similarity);
            }
        }
        return sortMapByValue(result);
    }
}
