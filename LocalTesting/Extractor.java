import lazo.sketch.LazoSketch;
import org.postgresql.util.PSQLException;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Extractor {
    private final DatabaseObject database_object;
    private final LazoSketch sketch = new LazoSketch();

    public Extractor(String url, String user, String password) throws SQLException {
        this.database_object = new DatabaseObject(url, user, password);
    }

    private List<Pair> listColumns() throws SQLException {
        List<Pair> result = new ArrayList<>();
        for (String table: database_object.listTables()) {
            if (table.equals("lookup_prediction")) continue;
            ResultSet columns = database_object.getColumns(table);
            while (columns.next()) {
                Pair pair = new Pair(table, columns.getString(4));
                result.add(pair);
            }
        }
        return result;
    }

    public List<Column> extractColumns() throws SQLException {
        List<Column> result = new ArrayList<>();
        List<Pair> columns = listColumns();
        for (Pair column: columns) {
            int data_type = getDataType(column);
            int avg_width = getAvgWidth(column);
            List<String> most_common_vals = getMostCommonVals(column);
            LazoSketch sketch = getSketch(column);
            result.add(new Column(column.table(), column.column(), data_type, avg_width, most_common_vals, sketch.getHashValues(), sketch.getCardinality()));
        }
        return result;
    }

    public int getDataType(Pair pair) throws SQLException {
        return database_object.getColumnDataType(pair);
    }

    public int getAvgWidth(Pair pair) throws SQLException {
        ResultSet avg_width_result = database_object.queryPgStatsColumn(pair, "avg_width");
        int avg_width = 0;
        if (avg_width_result.next())
            avg_width = avg_width_result.getInt(1);
        return avg_width;
    }

    public List<String> getMostCommonVals(Pair pair) throws SQLException {
        ResultSet most_common_vals_result = database_object.queryPgStatsColumn(pair, "most_common_vals");
        Array most_common_vals = null;
        if (most_common_vals_result.next())
            most_common_vals = most_common_vals_result.getArray(1);
        String[] most_common_vals_list = new String[0];
        if (most_common_vals != null) {
            most_common_vals_list = most_common_vals.toString().replaceAll("[{}]", "").split(",");
        }
        return new ArrayList<>(List.of(most_common_vals_list));
    }

    public LazoSketch getSketch(Pair pair) throws SQLException {
        ResultSet values = database_object.getColumnData(pair);
        int type = getDataType(pair);
        if (DatabaseObject.isInteger(type)) {
            while (values.next()) {
                int value;
                try {
                    value = values.getInt(1);
                } catch (PSQLException e) {
                    value = values.getBoolean(1) ? 1 : 0;
                }
                if (!values.wasNull())
                    sketch.update(String.valueOf(value));
            }
        }
        if (DatabaseObject.isDouble(type)) {
            while (values.next()) {
                int value = (int) values.getDouble(1);
                if (!values.wasNull())
                    sketch.update(String.valueOf(value));
            }
        }
        if (DatabaseObject.isString(type)) {
            while (values.next()) {
                String value = values.getString(1);
                if (!values.wasNull())
                    sketch.update(value);
            }
        }
        return sketch;
    }
}
