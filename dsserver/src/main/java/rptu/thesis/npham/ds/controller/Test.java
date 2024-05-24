package rptu.thesis.npham.ds.controller;

import com.univocity.parsers.common.TextParsingException;
import rptu.thesis.npham.ds.model.similarity.MeasureEnum;
import tech.tablesaw.api.Table;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import rptu.thesis.npham.ds.model.metadata.Metadata;
import rptu.thesis.npham.ds.model.similarity.Measure;
import rptu.thesis.npham.ds.model.similarity.MeasureList;
import rptu.thesis.npham.ds.model.similarity.SimilarityScores;
import rptu.thesis.npham.ds.model.sketch.Sketches;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SimilarityScoresRepo;
import rptu.thesis.npham.ds.repository.SketchesRepo;
import rptu.thesis.npham.ds.utils.CSVReader;
import rptu.thesis.npham.ds.service.lazo.Lazo;
import rptu.thesis.npham.ds.service.Profiler;
import rptu.thesis.npham.ds.service.SimilarityCalculator;
import rptu.thesis.npham.ds.utils.Jaccard;
import rptu.thesis.npham.ds.utils.Pair;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private final SimilarityCalculator similarity_calculator;
    private final Lazo lazo;

    @Autowired
    public Test(MetadataRepo metadata_repository, SketchesRepo sketches_repository, SimilarityScoresRepo similarity_scores_repository, Profiler profiler, SimilarityCalculator similarity_calculator, Lazo lazo) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.similarity_scores_repository = similarity_scores_repository;
        this.profiler = profiler;
        this.similarity_calculator = similarity_calculator;
        this.lazo = lazo;
    }

    @GetMapping("import")
    public String importAll() {
        String folderPath = "C:\\Users\\alexa\\Desktop\\Evaluation\\nextiajd\\training\\datasets";
        List<Pair<Metadata, Sketches>> all = new ArrayList<>();
        try {
            Stream<Path> paths = Files.list(Paths.get(folderPath));
            List<Path> files = paths.toList();
            for (Path file: files) {
                System.out.println("Reading " + file.toString());
                Table table;
                try {
                    table = CSVReader.readTable(file, true);
                } catch (ArrayIndexOutOfBoundsException | TextParsingException e2) {
                    System.out.println("Error reading file, skipping");
                    continue;
                }
                List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table, InetAddress.getLocalHost().getHostAddress());
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
        sketches.forEach(lazo::updateIndex);
//        metadatas.forEach(this::storeSimilarityScores);
        return "Import method finished";
    }

    @GetMapping("import/{filename}")
    public String importFile(@PathVariable String filename) throws UnknownHostException {
        String folderPath = "C:\\Users\\alexa\\Desktop\\testbedXS\\datasets";
        String filePath = folderPath + "\\" + filename;
        Table table;
        try {
            table = CSVReader.readTable(Paths.get(filePath), true);
        } catch (IndexOutOfBoundsException | TextParsingException e2) {
            return "Error reading file";
        }
        List<Pair<Metadata, Sketches>> metadata_list = profiler.profile(table, InetAddress.getLocalHost().getHostAddress());
        List<Pair<Metadata, Sketches>> all = new ArrayList<>(metadata_list);
        List<Metadata> metadatas = new ArrayList<>();
        List<Sketches> sketches = new ArrayList<>();
        all.forEach(pair -> {
            metadatas.add(pair.first());
            sketches.add(pair.second());
        });
        metadata_repository.saveAll(metadatas);
        sketches_repository.saveAll(sketches);
//        metadatas.forEach(this::storeSimilarityScores);
        return "Import method finished";
    }

    private void storeSimilarityScores(Metadata metadata) {
        List<SimilarityScores> scores_list = new ArrayList<>();

        String table_name = metadata.getTableName();
        String column_name = metadata.getColumnName();

        Map<Metadata, Jaccard> containment_similarity = similarity_calculator.columnValuesSimilarity(metadata);
        Set<Metadata> containment_candidate = containment_similarity.keySet();

        Map<Metadata, Jaccard> format_similarity = similarity_calculator.columnFormatSimilarity(metadata);
        format_similarity.keySet().retainAll(containment_candidate);

        Map<Metadata, Double> table_name_similarity = containment_candidate.stream().parallel().
                collect(Collectors.toMap(Function.identity(), m -> similarity_calculator.sentenceSimilarity(m.getTableName(), table_name)));

        Map<Metadata, Double> column_name_similarity = containment_candidate.stream().parallel().
                collect(Collectors.toMap(Function.identity(), m -> similarity_calculator.sentenceSimilarity(m.getColumnName(), column_name)));
        SimilarityScores scores = new SimilarityScores(metadata.getId(), new HashMap<>());

        for (Metadata m: containment_candidate) {
            SimilarityScores candidate_scores = similarity_scores_repository.findById(m.getId()).
                    orElse(new SimilarityScores(m.getId(), new HashMap<>()));

            double table_name_sim = table_name_similarity.get(m);
            Measure table_name_measure = new Measure(MeasureEnum.TABLE_NAME_WORDNET, table_name_sim, 1);
            double column_name_sim = column_name_similarity.get(m);
            Measure column_name_measure = new Measure(MeasureEnum.COLUMN_NAME_WORDNET, column_name_sim, 1);
            double containment_sim = containment_similarity.get(m).jcx();
            Measure containment_measure = new Measure(MeasureEnum.COLUMN_VALUE, containment_sim, 1);
            double format_sim = format_similarity.getOrDefault(m, new Jaccard(0, 0, 0)).jcx();
            Measure format_measure = new Measure(MeasureEnum.COLUMN_FORMAT, format_sim, 1);
            MeasureList measure_list = new MeasureList(new ArrayList<>());
            measure_list.addMeasure(table_name_measure);
            measure_list.addMeasure(column_name_measure);
            measure_list.addMeasure(containment_measure);
            measure_list.addMeasure(format_measure);
            scores.getScoreMap().put(m.getId(), measure_list);
            scores_list.add(scores);

            double containment_sim_candidate = containment_similarity.get(m).jcy();
            Measure containment_measure_candidate = new Measure(MeasureEnum.COLUMN_VALUE, containment_sim_candidate, 1);
            double format_sim_candidate = format_similarity.getOrDefault(m, new Jaccard(0, 0, 0)).jcy();
            Measure format_measure_candidate = new Measure(MeasureEnum.COLUMN_FORMAT, format_sim_candidate, 1);
            MeasureList measure_list_candidate = new MeasureList(new ArrayList<>());
            measure_list_candidate.addMeasure(table_name_measure);
            measure_list_candidate.addMeasure(column_name_measure);
            measure_list_candidate.addMeasure(containment_measure_candidate);
            measure_list_candidate.addMeasure(format_measure_candidate);
            candidate_scores.getScoreMap().put(metadata.getId(), measure_list_candidate);
            scores_list.add(candidate_scores);
        }

        similarity_scores_repository.saveAll(scores_list);
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
        similarity_scores_repository.deleteById(id);
        lazo.removeSketchFromIndex(id);
        return "Deleted " + id;
    }
}
