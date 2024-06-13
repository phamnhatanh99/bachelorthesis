package rptu.thesis.npham.dscommon.utils;

import com.univocity.parsers.common.TextParsingException;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.AddCellToColumnException;
import tech.tablesaw.io.ColumnIndexOutOfBoundsException;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvWriteOptions;

import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static tech.tablesaw.api.ColumnType.*;

public class CSV {

    private static final String query_results_csv = "C:\\Users\\alexa\\Desktop\\Evaluation\\results\\query_results.csv";
    private static final String sketching_results_csv = "C:\\Users\\alexa\\Desktop\\Evaluation\\results\\sketching_results.csv";

    public static Table readTable(Path path, boolean header) throws ArrayIndexOutOfBoundsException, TextParsingException {
        Table table;
        CsvReadOptions.Builder options = CsvReadOptions.builder(path.toString()).maxCharsPerColumn(32767).header(header);
        try {
            table = Table.read().csv(options.separator(',').build());
        } catch (IllegalArgumentException | ColumnIndexOutOfBoundsException e) {
            table = Table.read().csv(options.separator(';').build());
        } catch (AddCellToColumnException e) {
            table = Table.read().csv(options.dateTimeFormat(DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a", Locale.ENGLISH)).separator(',').build());
        }
        table.setName(trimCSVSuffix(table.name()));
        return table;
    }

    public static String trimCSVSuffix(String file_name) {
        if (!file_name.toLowerCase().endsWith(".csv"))
            return file_name;
        return file_name.toLowerCase().strip().substring(0, file_name.length() - 4);
    }

    public static void writeQueryResults(String table_name, boolean is_join, int limit, double threshold, int result_size, double elapsed, int tp, int fp, int fn, double precision, double recall, int tp_table, int fp_table, int fn_table, double precision_table, double recall_table) {
        // table_name, query_mode, limit, threshold, result_size, time_ms, tp, fp, fn, precision, recall, tp_table, fp_table, fn_table, precision_table, recall_table
        ColumnType[] types = {
                    STRING,
                    STRING,
                    INTEGER,
                    DOUBLE,
                    INTEGER,
                    DOUBLE,
                    INTEGER,
                    INTEGER,
                    INTEGER,
                    DOUBLE,
                    DOUBLE,
                    INTEGER,
                    INTEGER,
                    INTEGER,
                    DOUBLE,
                    DOUBLE};
        Table table;
        try {
            table = Table.read().csv(CsvReadOptions.builder(query_results_csv).columnTypes(types).header(true).separator(',').build());
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        String[] row = {
                table_name,
                is_join ? "join" : "union",
                limit + "",
                threshold + "",
                result_size + "",
                elapsed + "",
                tp + "",
                fp + "",
                fn + "",
                precision + "",
                recall + "",
                tp_table + "",
                fp_table + "",
                fn_table + "",
                precision_table + "",
                recall_table + ""};
        for (int i = 0; i < table.columnCount(); i++) {
            Column<?> column = table.column(i);
            column.appendCell(row[i]);
        }
        try {
            table.write().csv(CsvWriteOptions.builder(query_results_csv).header(true).separator(',').build());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeSketchingResults(String table_name, String column_name, double table_name_time, double column_name_time, double column_values_time, double column_format_time, double total_time) {
        ColumnType[] types = {STRING, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE};
        Table table;
        try {
            table = Table.read().csv(CsvReadOptions.builder(sketching_results_csv).columnTypes(types).header(true).separator(',').build());
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        String[] row = {table_name,
                        column_name,
                        table_name_time + "",
                        column_name_time + "",
                        column_values_time + "",
                        column_format_time + "",
                        total_time + ""};
        for (int i = 0; i < table.columnCount(); i++) {
            Column<?> column = table.column(i);
            column.appendCell(row[i]);
        }
        try {
            table.write().csv(CsvWriteOptions.builder(sketching_results_csv).header(true).separator(',').build());
            System.out.println("Results exported to " + sketching_results_csv);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
