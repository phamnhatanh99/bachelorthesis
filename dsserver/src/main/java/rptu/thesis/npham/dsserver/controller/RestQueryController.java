package rptu.thesis.npham.dsserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.dscommon.model.dto.RequestObject;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.query.QueryResults;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dsserver.evaluation.Datasets;
import rptu.thesis.npham.dsserver.evaluation.Evaluator;
import rptu.thesis.npham.dsserver.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.dsserver.exceptions.TableNotFoundException;
import rptu.thesis.npham.dsserver.model.similarity.Measure;
import rptu.thesis.npham.dsserver.model.similarity.MeasureType;
import rptu.thesis.npham.dsserver.model.similarity.Measures;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.service.LSHIndex;
import rptu.thesis.npham.dsserver.service.SimilarityCalculator;
import rptu.thesis.npham.dsserver.utils.Jaccard;

import java.util.*;

@RestController
public class RestQueryController {

    private static final Map<MeasureType, Double> QUERY_MEASURES = new HashMap<>();

    static {
//        QUERY_MEASURES.put(MeasureType.TABLE_NAME_WORDNET, 1d);
//        QUERY_MEASURES.put(MeasureType.COLUMN_NAME_WORDNET, 2d);
//        QUERY_MEASURES.put(MeasureType.TABLE_NAME_SHINGLE, 1d);
//        QUERY_MEASURES.put(MeasureType.COLUMN_NAME_SHINGLE, 3d);
        QUERY_MEASURES.put(MeasureType.COLUMN_VALUE, 3d);
        QUERY_MEASURES.put(MeasureType.COLUMN_FORMAT, 2d);
    }

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final SimilarityCalculator similarity_calculator;
    private final Evaluator evaluator;
    private final LSHIndex lsh_index;

    @Autowired
    public RestQueryController(MetadataRepo metadata_repository, SketchesRepo sketches_repository, SimilarityCalculator similarity_calculator, Evaluator evaluator, LSHIndex lsh_index) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.similarity_calculator = similarity_calculator;
        this.evaluator = evaluator;
        this.lsh_index = lsh_index;
    }

    @PostMapping("/query")
    public QueryResults query(@RequestBody List<RequestObject> request_objects,
                              @RequestParam("limit") Optional<Integer> limit,
                              @RequestParam("threshold") Optional<Double> threshold) {
        boolean existed = false;
        Metadata first_col = request_objects.get(0).metadata();
        String table_id = first_col.getId().split(Constants.SEPARATOR, 2)[0];

        for (RequestObject request_object : request_objects) {
            try {
                metadata_repository.save(request_object.metadata());
                sketches_repository.save(request_object.sketches());
                lsh_index.updateIndex(request_object.sketches());
            } catch (DuplicateKeyException e) { // Table already exists in the database, query from the database instead
                existed = true;
                break;
            }
        }

        List<Metadata> columns;
        if (!existed) {
            columns = metadata_repository.findByIdStartsWith(table_id);
            if (columns.isEmpty()) throw new TableNotFoundException();
        }
        else {
            Optional<Metadata> sample_col =
                    metadata_repository.findByUniqueIndex(first_col.getTableName(), first_col.getColumnName(), first_col.getType(), first_col.getSize(), first_col.getArity());
            String table_id_sample = sample_col.orElseThrow(MetadataNotFoundException::new).getId().split(Constants.SEPARATOR, 2)[0];
            columns = metadata_repository.findByIdStartsWith(table_id_sample);
        }


        QueryResults results = new QueryResults(new ArrayList<>());

        for (Metadata metadata : columns)
            results.addAll(queryUsingCertainMeasures(metadata.getId()));

        results = results.sortResults();
        if (threshold.isPresent()) results = results.withThreshold(threshold.get());
        if (limit.isPresent()) results = results.limitResults(limit.get());

        evaluate(results);

        if (!existed) {
            metadata_repository.deleteByIdStartsWith(table_id);
            sketches_repository.deleteByIdStartsWith(table_id);
            request_objects.forEach(request_object -> lsh_index.removeSketchFromIndex(request_object.sketches().getId()));
        }

        return results;
    }

    private QueryResults queryUsingCertainMeasures(String id) {
        QueryResults results = new QueryResults(new ArrayList<>());
        Metadata metadata = metadata_repository.findById(id).orElseThrow(MetadataNotFoundException::new);
        List<String> query_similar_types = Constants.typeInList(metadata.getType());
        String table_id = metadata.getId().split(Constants.SEPARATOR, 2)[0];

        if (MeasureType.onlyWordNet(QUERY_MEASURES.keySet())) {
            List<String> similar_types = Constants.typeInList(metadata.getType());
            List<Metadata> candidates = metadata_repository.findByIdNotStartsWithAndTypeIn(table_id, similar_types);
            for (Metadata candidate : candidates) {
                Measures measure_list = new Measures(new ArrayList<>());
                for (MeasureType measure : QUERY_MEASURES.keySet()) {
                    switch (measure) {
                        case TABLE_NAME_WORDNET -> {
                            double sim = similarity_calculator.sentenceSimilarity(metadata.getTableName(), candidate.getTableName());
                            Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                            measure_list.addMeasure(m);
                        }
                        case COLUMN_NAME_WORDNET -> {
                            double sim = similarity_calculator.sentenceSimilarity(metadata.getColumnName(), candidate.getColumnName());
                            Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                            measure_list.addMeasure(m);
                        }
                    }
                }
                results.add(metadata.toString(), candidate.toString(), measure_list.average());
            }
        }
        else if (MeasureType.onlyLSH(QUERY_MEASURES.keySet())) {
            Map<MeasureType, Map<Metadata, Jaccard>> candidates = new HashMap<>();
            for (MeasureType measure : QUERY_MEASURES.keySet()) {
                switch (measure) {
                    case TABLE_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryTableName(metadata));
                    case COLUMN_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryColumnName(metadata));
                    case COLUMN_VALUE -> candidates.put(measure, lsh_index.queryColumnValue(metadata));
                    case COLUMN_FORMAT -> candidates.put(measure, lsh_index.queryFormat(metadata));
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
                Measures measure_list = new Measures(new ArrayList<>());
                for (MeasureType measure : QUERY_MEASURES.keySet()) {
                    double sim = candidates.get(measure).getOrDefault(candidate, default_jaccard).jcx();
                    Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                    measure_list.addMeasure(m);
                }
                results.add(metadata.toString(), candidate.toString(), measure_list.average());
            }
        }
        else {
            Map<MeasureType, Map<Metadata, Jaccard>> candidates = new HashMap<>();
            for (MeasureType measure : QUERY_MEASURES.keySet()) {
                switch (measure) {
                    case TABLE_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryTableName(metadata));
                    case COLUMN_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryColumnName(metadata));
                    case COLUMN_VALUE -> candidates.put(measure, lsh_index.queryColumnValue(metadata));
                    case COLUMN_FORMAT -> candidates.put(measure, lsh_index.queryFormat(metadata));
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
                Measures measure_list = new Measures(new ArrayList<>());
                for (MeasureType measure : QUERY_MEASURES.keySet()) {
                    if (MeasureType.isLSH(measure)) {
                        double sim = candidates.get(measure).getOrDefault(candidate, default_jaccard).jcx();
                        Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                        measure_list.addMeasure(m);
                    }
                    else {
                        switch (measure) {
                            case TABLE_NAME_WORDNET -> {
                                double sim = similarity_calculator.sentenceSimilarity(metadata.getTableName(), candidate.getTableName());
                                Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                                measure_list.addMeasure(m);
                            }
                            case COLUMN_NAME_WORDNET -> {
                                double sim = similarity_calculator.sentenceSimilarity(metadata.getColumnName(), candidate.getColumnName());
                                Measure m = new Measure(measure, sim, QUERY_MEASURES.get(measure));
                                measure_list.addMeasure(m);
                            }
                        }
                    }
                }
                results.add(metadata.toString(), candidate.toString(), measure_list.average());
            }
        }
        return results;
    }

    private void evaluate(QueryResults results) {
        evaluator.loadGroundTruth(Datasets.NEXTIAJD_XS);
        System.out.println("Returned " + results.results().size() + " results");
        double[] eval = evaluator.precisionAndRecall(results);
        System.out.println("Precision: " + eval[0]);
        System.out.println("Recall: " + eval[1]);
        System.out.println("F1: " + evaluator.f1Score(eval[0], eval[1]));
    }
}
