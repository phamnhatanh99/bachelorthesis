package rptu.thesis.npham.ds.controller;

import com.univocity.parsers.common.TextParsingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.repository.MetadataRepository;
import rptu.thesis.npham.ds.service.Profiler;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.ColumnIndexOutOfBoundsException;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@RestController
public class Test {
    private final MetadataRepository metadata_repository;

    @Autowired
    public Test(MetadataRepository metadata_repository) {
        this.metadata_repository = metadata_repository;
    }

    @GetMapping("import")
    public void importAll() {
        String folderPath = "C:\\Users\\alexa\\Desktop\\testbedXS\\datasets";
        List<Metadata> all = new ArrayList<>();
        try {
            Stream<Path> paths = Files.list(Paths.get(folderPath));
            List<Path> files = paths.toList();
            for (Path file: files) {
                System.out.println("Reading " + file.toString());
                Table table;
                try {
                    table = readTable(file);
                } catch (ArrayIndexOutOfBoundsException | TextParsingException e2) {
                    continue;
                }
                List<Metadata> metadata_list = Profiler.profile(table);
                all.addAll(metadata_list);
            }
            paths.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        metadata_repository.insert(all);
        System.out.println("Method finished");
    }

    private Table readTable(Path path) throws ArrayIndexOutOfBoundsException, TextParsingException {
        Table table;
        CsvReadOptions.Builder options = CsvReadOptions.builder(path.toString()).maxCharsPerColumn(8192);
        try {
            table = Table.read().csv(options.separator(',').build());
        } catch (ColumnIndexOutOfBoundsException | IllegalArgumentException e) {
            table = Table.read().csv(options.separator(';').build());
        }
        return table;
    }

    @GetMapping("clear")
    public void clear() {
        metadata_repository.deleteAll();
    }
}
