import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Connects with DB and executes SQL queries.
 */
public class Querier {

    private final Connection connection;
    private final DatabaseMetaData databaseMetaData;

    /**
     * Constructs a new instance that connects to the TPC-H database.
     */
    public Querier() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/tpch";
        String user = "postgres";
        String password = "1234";
        connection = DriverManager.getConnection(url, user, password);
        databaseMetaData = connection.getMetaData();
    }

    /**
     * Returns a ResultSet that contain information of the columns of a given table.
     * @param name Name of the table
     * @return Information about the columns of the table
     */
    private ResultSet getColumns(String name) throws SQLException {
        return databaseMetaData.getColumns(null, null, name, null);
    }

    /**
     * Returns the datatype of the given column
     *
     * @param name Name of the column
     * @return The column's datatype
     */
    private int getColumnDataType(String name) throws SQLException {
        ResultSet columns = getColumns(TPCH.getTableName(name));
        int type = Types.NULL;
        while (columns.next()) {
            String column_name = columns.getString(4);
            if (column_name.equals(name)) {
                type = columns.getInt(5);
                break;
            }
        }
        return type;
    }

    private boolean isNumeric(String name) throws SQLException {
        return switch (getColumnDataType(name)) {
            case Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> true;
            default -> false;
        };
    }

    /**
     * Returns all other columns that has the same datatype as the columns in the given table.
     * @param name Name of the table
     * @return Mapping from each column in the input table to other columns with the same data type
     */
    public Map<String, Set<String>> compareColumnsByType(String name) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        ResultSet input_columns = getColumns(name);
        while (input_columns.next()) {
            String input_table_column_name = input_columns.getString(3);
            String input_column_name = input_columns.getString(4);
            String input_column_type = input_columns.getString(6);
            Set<String> similar_columns = new HashSet<>();
            result.put(input_column_name, similar_columns);
            for (String table_name : TPCH.stringList()) {
                ResultSet columns = getColumns(table_name);
                while (columns.next()) {
                    String table_column_name = columns.getString(3);
                    if (table_column_name.equals(input_table_column_name))
                        continue;
                    String column_name = columns.getString(4);
                    String column_type = columns.getString(6);
                    if (input_column_type.equals(column_type))
                        similar_columns.add(column_name);

                }
            }
        }
        return result;
    }

    /**
     * Returns a ResultSet that contains statistics of all tables.
     * @return A ResultSet that contains statistics of all tables
     */
    private ResultSet queryPgStats() throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String query = "SELECT t.* FROM pg_catalog.pg_stats t WHERE schemaname = 'public' ORDER BY tablename";
        return statement.executeQuery(query);
    }

    /**
     * Sort a map by its values.
     * @param map The map to be sorted
     * @return A LinkedHashMap sorted by values
     */
    private Map<String, Double> sortMap(Map<String, Double> map) {
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
     * @param name Name of the column
     * @param similar Name of the columns to rank
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByHistogramBounds(String name, Set<String> similar) throws SQLException {
        if (!isNumeric(name)) return new HashMap<>();
        Map<String, Double> result = new HashMap<>();
        ResultSet pg_stats = queryPgStats();
        double input_bounds_distance = 0;
        while (pg_stats.next()) {
            String attname = pg_stats.getString("attname");
            if (attname.equals(name)) {
                try {
                    Array input_bounds = pg_stats.getArray("histogram_bounds");
                    String temp = input_bounds.toString().replaceAll("[{}]", "");
                    String[] input_bounds_s_list = temp.split(",");
                    List<Float> input_bounds_list = Arrays.stream(input_bounds_s_list).map(Float::parseFloat).toList();
                    float lower = input_bounds_list.get(0);
                    float upper = input_bounds_list.get(input_bounds_list.size() - 1);
                    input_bounds_distance = upper - lower;
                } catch (NullPointerException exception) {
                    return result;
                }
                break;
            }
        }
        for (String similar_name : similar) {
            pg_stats.first();
            pg_stats.previous();
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_name)) {
                    try {
                        Array bounds = pg_stats.getArray("histogram_bounds");
                        String temp = bounds.toString().replaceAll("[{}]", "");
                        String[] input_bounds_s_list = temp.split(",");
                        List<Float> bounds_list = Arrays.stream(input_bounds_s_list).map(Float::parseFloat).toList();
                        float lower = bounds_list.get(0);
                        float upper = bounds_list.get(bounds_list.size() - 1);
                        double bounds_distance = upper - lower;
                        double similarity = Math.abs(input_bounds_distance - bounds_distance);
                        result.put(similar_name, similarity);
                    } catch (NullPointerException ignored) {
                    }
                }
            }
        }
        return sortMap(result);
    }

    /**
     * Ranks the similarity of a column with similar columns according to avg_width.
     * Similarity is calculated by distance the 2 avg_widths.
     * @param name Name of the column
     * @param similar Name of the columns to rank
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByAvgWidth(String name, Set<String> similar) throws SQLException {
        Map<String, Double> result = new HashMap<>();
        ResultSet pg_stats = queryPgStats();
        int input_avg_width = 0;
        while (pg_stats.next()) {
            String attname = pg_stats.getString("attname");
            if (attname.equals(name)) {
                input_avg_width = pg_stats.getInt("avg_width");
                break;
            }
        }
        for (String similar_name : similar) {
            pg_stats.first();
            pg_stats.previous();
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_name)) {
                    int avg_width = pg_stats.getInt("avg_width");
                    double similarity = Math.abs(input_avg_width - avg_width);
                    result.put(similar_name, similarity);
                }
            }
        }
        return sortMap(result);
    }

    /**
     * Ranks the similarity of a column with similar columns according to most_common_freqs.
     * Similarity is calculated by the Jaccard coefficient between the two most_common_freqs.
     * @param name Name of the column
     * @param similar Name of the columns to rank
     * @return Mapping from similarity values to the column names (from similar)
     */
    public Map<String, Double> rankColumnByMostCommonFreqs(String name, Set<String> similar) throws SQLException {
        Map<String, Double> result = new HashMap<>();
        ResultSet pg_stats = queryPgStats();
        Set<Float> input_most_common_freqs_set = new HashSet<>();
        while (pg_stats.next()) {
            String attname = pg_stats.getString("attname");
            if (attname.equals(name)) {
                try {
                    Array input_most_common_freqs = pg_stats.getArray("most_common_freqs");
                    input_most_common_freqs_set.addAll(List.of((Float[])
                            input_most_common_freqs.getArray()));
                }
                catch (NullPointerException exception) {
                    return result;
                }
                break;
            }
        }
        for (String similar_name : similar) {
            pg_stats.first();
            pg_stats.previous();
            while (pg_stats.next()) {
                String attname = pg_stats.getString("attname");
                if (attname.equals(similar_name)) {
                    try {
                        Array most_common_freqs = pg_stats.getArray("most_common_freqs");
                        Set<Float> most_common_freqs_set = new HashSet<>(List.of((Float[])
                                most_common_freqs.getArray()));
                        Set<Float> intersection = new HashSet<>(input_most_common_freqs_set);
                        intersection.retainAll(most_common_freqs_set);
                        Set<Float> union = new HashSet<>(input_most_common_freqs_set);
                        union.addAll(most_common_freqs_set);
                        if (!union.isEmpty()) {
                            double similarity = ((double) intersection.size()) / union.size();
                            if (similarity != 0)
                                result.put(similar_name, - similarity);
                        }
                    }
                    catch (NullPointerException ignored) {
                    }
                }
            }
        }
        return sortMap(result);
    }

    /**
     * Returns the cardinalities of all tables.
     * @return Mapping from table names to their cardinalities
     */
    public Map<String, Integer> getTablesCardinality() throws SQLException {
        Map<String, Integer> result = new HashMap<>();
        for (String name : TPCH.stringList()) {
            Statement statement = connection.createStatement();
            String query = "SELECT reltuples FROM pg_class WHERE relname = '" + name + "'";
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            Integer cardinality = resultSet.getInt("reltuples");
            result.put(name, cardinality);
        }
        return result;
    }

}
