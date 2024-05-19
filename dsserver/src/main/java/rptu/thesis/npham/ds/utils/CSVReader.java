package rptu.thesis.npham.ds.utils;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Service
public class CSVReader {
    public static Table getTable(MultipartFile file, List<String> column_types, String separator) {
        try {
            InputStream input_stream = file.getInputStream();
            String file_name = trimCSVSuffix(Objects.requireNonNull(file.getOriginalFilename()));
            ColumnType[] tablesaw_type = convertToColumnType(column_types);
            return Table.read().
                    usingOptions(CsvReadOptions.
                            builder(input_stream).
                            separator(separator.charAt(0)).
                            columnTypes(tablesaw_type).
                            tableName(file_name).
                            maxCharsPerColumn(100000));
        } catch (IOException e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
            return null;
        }
    }

    public static List<String> getColumnNames(Table table) {
        return table.columnNames();
    }

    public static String trimCSVSuffix(String file_name) {
        return file_name.toLowerCase().strip().substring(0, file_name.length() - 4);
    }

    public static ColumnType[] convertToColumnType(List<String> column_types) {
        int size = column_types.size();
        ColumnType[] result = new ColumnType[size];
        for (int i = 0; i < size; i++) {
            String type = column_types.get(i);
            switch (type.toLowerCase()) {
                case "boolean" -> result[i] = ColumnType.BOOLEAN;
                case "text" -> result[i] = ColumnType.TEXT;
                case "integer" -> result[i] = ColumnType.INTEGER;
                case "double" -> result[i] = ColumnType.DOUBLE;
            }
        }
        return result;
    }
}
