package rptu.thesis.npham.ds.controller;

import com.univocity.parsers.common.TextParsingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.model.Sketches;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SketchesRepo;
import rptu.thesis.npham.ds.service.CSV;
import rptu.thesis.npham.ds.service.Profiler;
import rptu.thesis.npham.ds.service.Similarity;
import rptu.thesis.npham.ds.utils.Pair;
import rptu.thesis.npham.ds.utils.StringUtils;
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
    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final Profiler profiler;
    private final Similarity similarity;

    @Autowired
    public Test(MetadataRepo metadata_repository, SketchesRepo sketches_repository, Profiler profiler, Similarity similarity) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.profiler = profiler;
        this.similarity = similarity;
    }

    @GetMapping("import")
    public String importAll() {
        String folderPath = "C:\\Users\\alexa\\Desktop\\testbedXS\\datasets";
        List<Pair<Metadata, Sketches>> all = new ArrayList<>();
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
                table.setName(CSV.trimCSVSuffix(table.name()));
                List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table);
                all.addAll(metadata_list);
            }
            paths.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<Metadata> metadatas = new ArrayList<>();
        List<Sketches> sketches = new ArrayList<>();
        all.forEach(pair -> {
            metadatas.add(pair.first());
            sketches.add(pair.second());
        });
        metadata_repository.saveAll(metadatas);
        sketches_repository.saveAll(sketches);
        return "Import method finished";
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
    public String clear() {
        metadata_repository.deleteAll();
        sketches_repository.deleteAll();
        return "Clear method finished";
    }

    @GetMapping("delete/{id}")
    public String delete(@PathVariable String id) {
        metadata_repository.deleteById(id);
        sketches_repository.deleteById(id);
        return "Deleted " + id;
    }

    @GetMapping("wordnet")
    public void wordnet() {
        String sentence1 = "geo local area";
        String sentence2 = "geom";
        System.out.println(similarity.sentenceSimilarity(sentence1, sentence2));
    }
}
