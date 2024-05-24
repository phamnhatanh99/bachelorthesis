package rptu.thesis.npham.ds.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.evaluation.Datasets;
import rptu.thesis.npham.ds.evaluation.Evaluator;
import rptu.thesis.npham.ds.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.ds.exceptions.NoScoreException;
import rptu.thesis.npham.ds.model.metadata.Metadata;
import rptu.thesis.npham.ds.model.query.QueryResults;
import rptu.thesis.npham.ds.model.similarity.Measure;
import rptu.thesis.npham.ds.model.similarity.Score;
import rptu.thesis.npham.ds.model.similarity.SimilarityMeasures;
import rptu.thesis.npham.ds.model.similarity.SimilarityScores;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SimilarityScoresRepo;
import rptu.thesis.npham.ds.service.SimilarityCalculator;
import rptu.thesis.npham.ds.service.lazo.Lazo;
import rptu.thesis.npham.ds.utils.Constants;
import rptu.thesis.npham.ds.utils.Jaccard;
import rptu.thesis.npham.ds.utils.StringUtils;

import java.util.*;

@RestController
public class QueryController {

    private static final Map<SimilarityMeasures, Double> QUERY_MEASURES = new HashMap<>();

    static {
//        QUERY_MEASURES.put(SimilarityMeasures.TABLE_NAME_WORDNET, 1d);
//        QUERY_MEASURES.put(SimilarityMeasures.COLUMN_NAME_WORDNET, 2d);
//        QUERY_MEASURES.put(SimilarityMeasures.TABLE_NAME_SHINGLE, 1d);
//        QUERY_MEASURES.put(SimilarityMeasures.COLUMN_NAME_SHINGLE, 3d);
        QUERY_MEASURES.put(SimilarityMeasures.COLUMN_VALUE, 3d);
        QUERY_MEASURES.put(SimilarityMeasures.COLUMN_FORMAT, 2d);
    }

    private final MetadataRepo metadata_repository;
    private final SimilarityScoresRepo similarity_scores_repository;
    private final SimilarityCalculator similarity_calculator;
    private final Evaluator evaluator;
    private final Lazo lazo;

    @Autowired
    public QueryController(MetadataRepo metadata_repository, SimilarityScoresRepo similarity_scores_repository, SimilarityCalculator similarity_calculator, Evaluator evaluator, Lazo lazo) {
        this.metadata_repository = metadata_repository;
        this.similarity_scores_repository = similarity_scores_repository;
        this.similarity_calculator = similarity_calculator;
        this.evaluator = evaluator;
        this.lazo = lazo;
    }

    @GetMapping("id/{id}")
    public QueryResults queryByID(@PathVariable String id) {
        Optional<SimilarityScores> query = similarity_scores_repository.findById(id);
        if (query.isEmpty()) throw new NoScoreException();
        SimilarityScores scores = query.get();
        Metadata metadata = metadata_repository.findById(id).orElseThrow(MetadataNotFoundException::new);

        QueryResults results = new QueryResults(new ArrayList<>());

        scores.getScoreMap().entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .filter(e -> e.getValue().average() > 0.0)
                .forEach(e -> {
                    Metadata m = metadata_repository.findById(e.getKey()).orElseThrow(MetadataNotFoundException::new);
                    double avg = e.getValue().average();
                    results.add(metadata.toString(), m.toString(), avg);
                });

        return results;
    }

    @GetMapping("table_id/{uuid}")
    public QueryResults queryTable(@PathVariable String uuid) {
        long start = System.currentTimeMillis();

        List<Metadata> columns = metadata_repository.findByIdStartsWith(uuid);
        if (columns.isEmpty()) throw new RuntimeException("Table not found");

        QueryResults results = new QueryResults(new ArrayList<>());

        for (Metadata metadata : columns) {
            try {
                results.addAll(queryUsingCertainMeasures(metadata.getId()));
            } catch (NoScoreException e) {
                System.out.println("No score for " + metadata.getTableName() + " - " + metadata.getColumnName());
            }
        }

        results.sortResults();
        results = results.withThreshold(0.5);
        results.limitResults(10);

        long end = System.currentTimeMillis();
        System.out.println("Time taken: " + (end - start) + "ms");

        evaluate(results);
        return results;
    }

    @GetMapping("alt/id/{id}")
    public QueryResults queryUsingCertainMeasures(@PathVariable String id) {
        QueryResults results = new QueryResults(new ArrayList<>());
        Metadata metadata = metadata_repository.findById(id).orElseThrow(MetadataNotFoundException::new);
        List<String> query_similar_types = Constants.typeInList(metadata.getType());
        String table_id = metadata.getId().split(StringUtils.SEPARATOR, 2)[0];

        if (SimilarityMeasures.onlyWordNet(QUERY_MEASURES.keySet())) {
            List<String> similar_types = Constants.typeInList(metadata.getType());
            List<Metadata> candidates = metadata_repository.findByIdNotStartsWithAndTypeIn(table_id, similar_types);
            for (Metadata candidate : candidates) {
                Score score = new Score(new ArrayList<>());
                for (SimilarityMeasures measure : QUERY_MEASURES.keySet()) {
                    switch (measure) {
                        case TABLE_NAME_WORDNET -> {
                            double sim = similarity_calculator.sentenceSimilarity(metadata.getTableName(), candidate.getTableName());
                            Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                            score.addMeasure(m);
                        }
                        case COLUMN_NAME_WORDNET -> {
                            double sim = similarity_calculator.sentenceSimilarity(metadata.getColumnName(), candidate.getColumnName());
                            Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                            score.addMeasure(m);
                        }
                    }
                }
                results.add(metadata.toString(), candidate.toString(), score.average());
            }
        }
        else if (SimilarityMeasures.onlyLSH(QUERY_MEASURES.keySet())) {
            Map<SimilarityMeasures, Map<Metadata, Jaccard>> candidates = new HashMap<>();
            for (SimilarityMeasures measure : QUERY_MEASURES.keySet()) {
               switch (measure) {
                   case TABLE_NAME_SHINGLE -> candidates.put(measure, lazo.queryTableName(metadata));
                   case COLUMN_NAME_SHINGLE -> candidates.put(measure, lazo.queryColumnName(metadata));
                   case COLUMN_VALUE -> candidates.put(measure, lazo.queryColumnValue(metadata));
                   case COLUMN_FORMAT -> candidates.put(measure, lazo.queryFormat(metadata));
               }
            }

            Jaccard default_jaccard = new Jaccard(0, 0, 0);
            Set<Metadata> candidate_set = new HashSet<>();
            for (Map<Metadata, Jaccard> candidate : candidates.values()) {
                for (Metadata m : candidate.keySet()) {
                    if (query_similar_types.contains(m.getType()))
                        candidate_set.add(m);
                }
            }

            for (Metadata candidate : candidate_set) {
                Score score = new Score(new ArrayList<>());
                for (SimilarityMeasures measure : QUERY_MEASURES.keySet()) {
                    double sim = candidates.get(measure).getOrDefault(candidate, default_jaccard).jcx();
                    Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                    score.addMeasure(m);
                }
                results.add(metadata.toString(), candidate.toString(), score.average());
            }
        }
        else {
            Map<SimilarityMeasures, Map<Metadata, Jaccard>> candidates = new HashMap<>();
            for (SimilarityMeasures measure : QUERY_MEASURES.keySet()) {
                switch (measure) {
                    case TABLE_NAME_SHINGLE -> candidates.put(measure, lazo.queryTableName(metadata));
                    case COLUMN_NAME_SHINGLE -> candidates.put(measure, lazo.queryColumnName(metadata));
                    case COLUMN_VALUE -> candidates.put(measure, lazo.queryColumnValue(metadata));
                    case COLUMN_FORMAT -> candidates.put(measure, lazo.queryFormat(metadata));
                }
            }

            Jaccard default_jaccard = new Jaccard(0, 0, 0);
            Set<Metadata> candidate_set = new HashSet<>();
            for (Map<Metadata, Jaccard> candidate : candidates.values()) {
                for (Metadata m : candidate.keySet()) {
                    if (query_similar_types.contains(m.getType()))
                        candidate_set.add(m);
                }
            }

            for (Metadata candidate : candidate_set) {
                Score score = new Score(new ArrayList<>());
                for (SimilarityMeasures measure : QUERY_MEASURES.keySet()) {
                    if (SimilarityMeasures.isLSH(measure)) {
                        double sim = candidates.get(measure).getOrDefault(candidate, default_jaccard).jcx();
                        Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                        score.addMeasure(m);
                    }
                    else {
                        switch (measure) {
                            case TABLE_NAME_WORDNET -> {
                                double sim = similarity_calculator.sentenceSimilarity(metadata.getTableName(), candidate.getTableName());
                                Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                                score.addMeasure(m);
                            }
                            case COLUMN_NAME_WORDNET -> {
                                double sim = similarity_calculator.sentenceSimilarity(metadata.getColumnName(), candidate.getColumnName());
                                Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                                score.addMeasure(m);
                            }
                        }
                    }
                }
                results.add(metadata.toString(), candidate.toString(), score.average());
            }
        }
        return results;
    }

    private void evaluate(QueryResults results) {
        evaluator.loadGroundTruth(Datasets.NEXTIAJD_TRAINING);
        System.out.println("Returned " + results.size() + " results");
        double[] eval = evaluator.precisionAndRecall(results);
        System.out.println("Precision: " + eval[0]);
        System.out.println("Recall: " + eval[1]);
        System.out.println("F1: " + evaluator.f1Score(eval[0], eval[1]));
    }
}
