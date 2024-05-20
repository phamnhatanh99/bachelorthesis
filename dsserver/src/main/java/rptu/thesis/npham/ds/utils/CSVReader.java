package rptu.thesis.npham.ds.utils;

import com.univocity.parsers.common.TextParsingException;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.ColumnIndexOutOfBoundsException;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.nio.file.Path;

public class CSVReader {

    public static Table readTable(Path path, boolean header) throws ArrayIndexOutOfBoundsException, TextParsingException {
        Table table;
        CsvReadOptions.Builder options = CsvReadOptions.builder(path.toString()).maxCharsPerColumn(32767).header(header);
        try {
            table = Table.read().csv(options.separator(',').build());
        } catch (ColumnIndexOutOfBoundsException | IllegalArgumentException | IndexOutOfBoundsException e) {
            System.out.println("Error reading file with comma separator");
            table = Table.read().csv(options.separator(';').build());
        }
        table.setName(trimCSVSuffix(table.name()));
        return table;
    }

    public static String trimCSVSuffix(String file_name) {
        return file_name.toLowerCase().strip().substring(0, file_name.length() - 4);
    }

}
