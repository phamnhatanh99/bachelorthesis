package rptu.thesis.npham.dscommon.utils;

import com.univocity.parsers.common.TextParsingException;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.AddCellToColumnException;
import tech.tablesaw.io.ColumnIndexOutOfBoundsException;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class CSVReader {

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

}
