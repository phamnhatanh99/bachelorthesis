package rptu.thesis.npham.ds.controller;

import com.univocity.parsers.common.TextParsingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.model.Score;
import rptu.thesis.npham.ds.model.SimilarityScores;
import rptu.thesis.npham.ds.model.Sketches;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SimilarityScoresRepo;
import rptu.thesis.npham.ds.repository.SketchesRepo;
import rptu.thesis.npham.ds.service.CSV;
import rptu.thesis.npham.ds.service.Lazo;
import rptu.thesis.npham.ds.service.Profiler;
import rptu.thesis.npham.ds.service.Similarity;
import rptu.thesis.npham.ds.utils.Jaccard;
import rptu.thesis.npham.ds.utils.Pair;
import rptu.thesis.npham.ds.utils.StringUtils;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.ColumnIndexOutOfBoundsException;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class Test {
    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final SimilarityScoresRepo similarity_scores_repository;
    private final Profiler profiler;
    private final Similarity similarity;
    private final Lazo lazo;

    @Autowired
    public Test(MetadataRepo metadata_repository, SketchesRepo sketches_repository, SimilarityScoresRepo similarity_scores_repository, Profiler profiler, Similarity similarity, Lazo lazo) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.similarity_scores_repository = similarity_scores_repository;
        this.profiler = profiler;
        this.similarity = similarity;
        this.lazo = lazo;
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

    @GetMapping("import/{filename}")
    public String importFile(@PathVariable String filename) {
        String folderPath = "C:\\Users\\alexa\\Desktop\\testbedXS\\datasets";
        String filePath = folderPath + "\\" + filename;
        Table table;
        try {
            table = readTable(Paths.get(filePath));
        } catch (ArrayIndexOutOfBoundsException | TextParsingException e2) {
            return "Error reading file";
        }
        table.setName(CSV.trimCSVSuffix(table.name()));
        List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table);
        List<Pair<Metadata, Sketches>> all = new ArrayList<>(metadata_list);
        List<Metadata> metadatas = new ArrayList<>();
        List<Sketches> sketches = new ArrayList<>();
        all.forEach(pair -> {
            metadatas.add(pair.first());
            sketches.add(pair.second());
        });
        metadata_repository.saveAll(metadatas);
        sketches_repository.saveAll(sketches);
        metadatas.forEach(m -> storeSimilarityScores(m.getId()));
        return "Import method finished";
    }

    private void storeSimilarityScores(String id) {
        Optional<Metadata> query = metadata_repository.findById(id);
        if (query.isEmpty()) throw new RuntimeException("ID does not exists");
        Metadata metadata = query.get();

        String table_name = metadata.getTableName();
        String column_name = metadata.getColumnName();

        Map<Metadata, Jaccard> containment_similarity = lazo.queryColumnContainment(metadata);
        Set<Metadata> containment_candidate = containment_similarity.keySet();

        Map<Metadata, Jaccard> format_similarity = lazo.queryFormatContainment(metadata);
        format_similarity.keySet().retainAll(containment_candidate);

        Map<Metadata, Double> table_name_similarity = containment_candidate.stream().
                collect(Collectors.toMap(Function.identity(), m -> similarity.sentenceSimilarity(m.getTableName(), table_name)));
        Map<Metadata, Double> column_name_similarity = containment_candidate.stream().
                collect(Collectors.toMap(Function.identity(), m -> similarity.sentenceSimilarity(m.getColumnName(), column_name)));

        List<SimilarityScores> scores_list = new ArrayList<>();

        SimilarityScores scores = new SimilarityScores(metadata.getId(), new HashMap<>());

        for (Metadata m: containment_candidate) {
            SimilarityScores candidate_scores = similarity_scores_repository.findById(m.getId()).
                    orElse(new SimilarityScores(m.getId(), new HashMap<>()));

            double table_name_sim = table_name_similarity.get(m);
            double column_name_sim = column_name_similarity.get(m);
            double containment_sim = containment_similarity.get(m).jcx();
            double format_sim = format_similarity.getOrDefault(m, new Jaccard(0, 0, 0)).jcx();
            scores.getScoreMap().put(m.getId(), new Score(table_name_sim, column_name_sim, containment_sim, format_sim, 0));
            scores_list.add(candidate_scores);

            double containment_sim_candidate = containment_similarity.get(m).jcy();
            double format_sim_candidate = format_similarity.getOrDefault(m, new Jaccard(0, 0, 0)).jcy();
            candidate_scores.getScoreMap().put(metadata.getId(), new Score(table_name_sim, column_name_sim, containment_sim_candidate, format_sim_candidate, 0));
            scores_list.add(candidate_scores);
        }

        similarity_scores_repository.saveAll(scores_list);
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
        similarity_scores_repository.deleteAll();
        lazo.clearIndexes();
        return "Clear method finished";
    }

    @GetMapping("delete/{id}")
    public String delete(@PathVariable String id) {
        metadata_repository.deleteById(id);
        sketches_repository.deleteById(id);
        lazo.removeSketchFromIndex(id);
        return "Deleted " + id;
    }

    @GetMapping("wordnet")
    public void wordnet() {
        String sentence1 = "geo local area";
        String sentence2 = "geom";
        System.out.println(similarity.sentenceSimilarity(sentence1, sentence2));
    }
}
