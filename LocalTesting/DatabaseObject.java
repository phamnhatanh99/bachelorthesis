import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class that contains all the methods to interact with the database.
 */
public class DatabaseObject {

    private final Connection connection;
    private final DatabaseMetaData database_metadata;
    private final List<String> tables;

    /**
     * Constructor for DatabaseObject.
     * @param url The url of the database
     * @param user The username of the database
     * @param password The password of the database
     */
    public DatabaseObject(String url, String user, String password) throws SQLException {
        // Connect to the database
        connection = DriverManager.getConnection(url, user, password);
        database_metadata = connection.getMetaData();

        // Store the tables' names in a list
        tables = new ArrayList<>();
        ResultSet tables_data = database_metadata.getTables(null, null, null, new String[] { "TABLE" });
        while (tables_data.next()) {
            tables.add(tables_data.getString(3));
        }
    }

    /**
     * Checks if the data type is numeric.
     * @param data_type The data type to check
     * @return True if the data type is numeric, false otherwise
     */
    public static boolean isNumeric(int data_type) {
        return switch (data_type) {
            case Types.BIGINT, Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.NUMERIC, Types.REAL, Types.SMALLINT, Types.TINYINT -> true;
            default -> false;
        };
    }

    /**
     * Returns the statistics from pg_stats of all tables.
     * @return A ResultSet that contains statistics of all tables
     */
    public ResultSet queryPgStats() throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String query = "SELECT t.* FROM pg_catalog.pg_stats t WHERE schemaname = 'public' ORDER BY tablename";
        return statement.executeQuery(query);
    }

    /**
     * Return the tables' names as list of string.
     * @return String list containing the tables' names
     */
    public List<String> listTables() {
        return tables;
    }

    /**
     * Return the list of tables as a string.
     * @return The string listing the tables' names
     */
    public String listTablesAsString() {
        return String.join(", ", listTables());
    }

    /**
     * Returns the cardinalities of all tables.
     * @return Mapping from table names to their cardinalities
     */
    public Map<String, Integer> getTablesCardinality() throws SQLException {
        Map<String, Integer> result = new HashMap<>();
        for (String name : listTables()) {
            Statement statement = connection.createStatement();
            String query = "SELECT reltuples FROM pg_class WHERE relname = '" + name + "'";
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next(); // Move the cursor to the first row
            Integer cardinality = resultSet.getInt("reltuples");
            result.put(name, cardinality);
        }
        return result;
    }

    /**
     * Returns a ResultSet that contain information of the columns of a given table.
     * @param table_name Name of the table
     * @return Information about the columns of the table. The ResultSet contains the following columns:
     * 3 - TABLE_NAME, 4 - COLUMN_NAME, 5 - DATA_TYPE, 6 - TYPE_NAME
     */
    public ResultSet getColumns(String table_name) throws SQLException {
        return database_metadata.getColumns(null, null, table_name, null);
    }

    /**
     * Returns the datatype of the given column from the given table.
     * @param table_name_and_column_name Name of the table joined with the name of the column by a dot
     * @return The column's datatype from java.sql.Types
     */
    public int getColumnDataType(String table_name_and_column_name) throws SQLException {
        String[] split = table_name_and_column_name.split("\\.", 2);
        String table_name = split[0];
        String column_name = split[1];
        ResultSet columns = getColumns(table_name);
        int type = Types.NULL;
        while (columns.next()) {
            String name = columns.getString(4);
            if (name.equals(column_name)) { // If the column name matches the given column name
                type = columns.getInt(5);
                break;
            }
        }
        return type;
    }
}
