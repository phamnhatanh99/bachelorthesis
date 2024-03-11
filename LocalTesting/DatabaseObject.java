import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
            case Types.BIGINT, Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.NUMERIC, Types.REAL, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.BOOLEAN -> true;
            default -> false;
        };
    }

    public static boolean isInteger(int data_type) {
        return switch (data_type) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.BOOLEAN -> true;
            default -> false;
        };
    }

    public static boolean isDouble(int data_type) {
        return switch (data_type) {
            case Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.NUMERIC, Types.REAL -> true;
            default -> false;
        };
    }

    public static boolean isDateOrTime(int data_type) {
        return data_type == Types.DATE || data_type == Types.TIME || data_type == Types.TIMESTAMP;
    }

    public static boolean isString(int data_type) {
        return switch (data_type) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> true;
            default -> false;
        };
    }

    public ResultSet queryPgStatsColumn(Pair table_and_column_name, String pg_stats_column) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String table_name = table_and_column_name.table();
        String column_name = table_and_column_name.column();
        String query = "SELECT " + pg_stats_column + " FROM pg_catalog.pg_stats WHERE tablename = '" + table_name + "' AND attname = '" + column_name + "'";
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
     * Returns the cardinality of a given table.
     * @param table_name Name of the table
     * @return The cardinality of the table
     */
    public int getTableCardinality(String table_name) throws SQLException {
        Statement statement = connection.createStatement();
        String query = "SELECT reltuples FROM pg_class WHERE relname = '" + table_name + "'";
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next(); // Move the cursor to the first row
        return resultSet.getInt("reltuples");
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
    public int getColumnDataType(Pair table_name_and_column_name) throws SQLException {
        String table_name = table_name_and_column_name.table();
        String column_name = table_name_and_column_name.column();
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

    public ResultSet getColumnData(Pair table_and_column_name) throws SQLException {
        Statement statement = connection.createStatement();
        String table_name = table_and_column_name.table();
        String column_name = table_and_column_name.column();
        String query = "SELECT " + column_name + " FROM " + table_name;
        return statement.executeQuery(query);
    }

    public void insertPrediction(Pair source, Pair target, double table_name, double column_name, double avg_width, double most_common_vals, double jaccard) throws SQLException {
        Statement statement = connection.createStatement();
        String query = "INSERT INTO lookup_prediction (source_table, source_column, target_table, target_column, table_name, column_name, avg_width, most_common_vals, jaccard_estimate) VALUES ('" + source.table() + "', '" + source.column() + "', '" + target.table() + "', '" + target.column() + "', " + table_name + ", " + column_name + ", " + avg_width + ", " + most_common_vals + ", " + jaccard + ") ON CONFLICT (source_table, source_column, target_table, target_column) DO UPDATE SET table_name = " + table_name + ", column_name = " + column_name + ", avg_width = " + avg_width + ", most_common_vals = " + most_common_vals + ", jaccard_estimate = " + jaccard;
        statement.execute(query);
    }
}
