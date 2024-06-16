package rptu.thesis.npham.dsserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.*;
import rptu.thesis.npham.dscommon.model.dto.Summaries;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.query.QueryResults;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.utils.MethodTimer;
import rptu.thesis.npham.dsserver.evaluation.Datasets;
import rptu.thesis.npham.dsserver.evaluation.QueryResultsEvaluator;
import rptu.thesis.npham.dsserver.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.dsserver.model.similarity.Measure;
import rptu.thesis.npham.dsserver.model.similarity.MeasureType;
import rptu.thesis.npham.dsserver.model.similarity.Measures;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.service.LSHIndex;
import rptu.thesis.npham.dsserver.service.SimilarityCalculator;
import rptu.thesis.npham.dsserver.utils.Score;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class QueryController {

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final LSHIndex lsh_index;
    private final SimilarityCalculator similarity_calculator;
    private final QueryResultsEvaluator evaluator;

    @Autowired
    public QueryController(MetadataRepo metadata_repository, SketchesRepo sketches_repository, SimilarityCalculator similarity_calculator, QueryResultsEvaluator evaluator, LSHIndex lsh_index) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.similarity_calculator = similarity_calculator;
        this.evaluator = evaluator;
        this.lsh_index = lsh_index;
    }

    @GetMapping("clearGT")
    public String clearGT() {
        evaluator.clearGroundTruth();
        return "Ground truth emptied";
    }

    @GetMapping("loadGT")
    public String loadGT() {
        for (Datasets dataset : Datasets.values()) {
            if (dataset.equals(Datasets.TUS_S)) continue;
            evaluator.loadGroundTruth(dataset);
        }
        return "GT loaded";
    }

    /**
     * Rest end point to query all the tables in the database
     */
    @GetMapping("/queryAll")
    public void queryAll() {
        List<Metadata> all_metadata = metadata_repository.findAll();
        Set<String> table_ids = all_metadata.stream().map(metadata ->
                metadata.getId().split(Constants.SEPARATOR, 2)[0])
                .collect(Collectors.toSet());
        for (String table_id : table_ids) {
            List<Metadata> columns = metadata_repository.findByIdStartsWith(table_id);
            List<Summaries> request_objects = new ArrayList<>();
            for (Metadata metadata : columns) {
                Sketches sketches = sketches_repository.findById(metadata.getId()).orElseThrow(MetadataNotFoundException::new);
                request_objects.add(new Summaries(metadata, sketches));
            }
            queryTable(request_objects, "join", Optional.of(100), 0d);
        }
    }

    /**
     * Rest end point to perform a dataset search
     * @param summaries Wrapper object that contains the metadatas and sketches of the columns of the query table
     * @param mode Query mode, either "join" or "union"
     * @param limit The maximum amount of results to be returned
     * @param threshold The minimum score where a candidate is considered a match
     * @return The results of a query
     */
    @PostMapping("/query")
    public QueryResults queryTable(@RequestBody List<Summaries> summaries,
                                   @RequestParam("mode") String mode,
                                   @RequestParam("limit") Optional<Integer> limit,
                                   @RequestParam("threshold") Double threshold) {

//        if (threshold == 0) {
//            queryTable(summaries, mode, limit, 0.2);
//            queryTable(summaries, mode, limit, 0.4);
//            queryTable(summaries, mode, limit, 0.6);
//            queryTable(summaries, mode, limit, 0.8);
//        }

        List<MeasureType> query_measures = new ArrayList<>();
        boolean is_join = false;

        // Manually set which measures to use for a query
        if (mode.equals("join")) {
            System.out.println("Join mode");
            is_join = true;
        } else {
            System.out.println("Union mode");
        }

        query_measures.add(MeasureType.TABLE_NAME_WORDNET);
        query_measures.add(MeasureType.COLUMN_NAME_WORDNET);
//        query_measures.add(MeasureType.TABLE_NAME_QGRAM);
//        query_measures.add(MeasureType.COLUMN_NAME_QGRAM);
        query_measures.add(MeasureType.COLUMN_VALUE);
        query_measures.add(MeasureType.COLUMN_FORMAT);

        Collections.sort(query_measures);

        MethodTimer timer = new MethodTimer("query table");
        timer.start();
        // Boolean to check if the table being queried already exists in DB
        boolean existed = false;
        // All metadatas in the request come from the same table, so only need to retrieve the table id from the first metadata
        Metadata first_col = summaries.get(0).metadata();
        String table_id = first_col.getId().split(Constants.SEPARATOR, 2)[0];

        // Attempt to save the summaries into DB for querying
        for (Summaries request_object : summaries) {
            try {
                metadata_repository.save(request_object.metadata());
            } catch (DuplicateKeyException e) { // Table already exists in the database, don't save and query from the database instead
                existed = true;
                break;
            }
            sketches_repository.save(request_object.sketches());
        }

        List<Metadata> columns;
        // Query table is not in DB, so we retrieve the metadatas from the request object
        if (!existed) {
            columns = summaries.stream().map(Summaries::metadata).collect(Collectors.toCollection(ArrayList::new));
        }
        // Query table is in DB, but the uuid generated is different, so we query the DB using the unique index instead
        else {
            columns = new ArrayList<>();
            for (Summaries request_object : summaries) {
                Metadata metadata = metadata_repository.findByUniqueIndex(
                        request_object.metadata().getTableName(),
                        request_object.metadata().getColumnName(),
                        request_object.metadata().getType(),
                        request_object.metadata().getSize(),
                        request_object.metadata().getArity())
                        .orElseThrow(MetadataNotFoundException::new);
                columns.add(metadata);
            }
        }

        QueryResults results = new QueryResults(new ArrayList<>());

        for (Metadata metadata : columns)
            results.addAll(queryColumn(metadata, is_join, query_measures, threshold));

        results = results.sortResults();
        results = results.withThreshold(threshold);
        if (limit.isPresent()) results = results.limitResults(limit.get());
        // Remove the query from the DB once finished
        if (!existed) {
            metadata_repository.deleteByIdStartsWith(table_id);
            sketches_repository.deleteByIdStartsWith(table_id);
        }

        double elapsed = timer.getElapsed();
        evaluator.evaluate(results, is_join, limit.orElse(Integer.MAX_VALUE), threshold, elapsed);
        return results;
    }

    /**
     * Perform similarity calculations based on the similarity measures available
     * @param metadata Column that needs to be queried
     * @param is_join Boolean to check if the query mode is a join or a union
     * @param query_measures The similarity measures used for the query
     * @return The query results
     */
    public QueryResults queryColumn(Metadata metadata, boolean is_join, List<MeasureType> query_measures, double threshold) {
        QueryResults results = new QueryResults(new ArrayList<>());
        // If only WordNet measures are used, find columns with similar types and calculate WordNet similarity among them
        if (MeasureType.onlyWordNet(query_measures))
            results.addAll(onlyWordNetQuery(metadata, query_measures, is_join, threshold));
        // If only LSH measures are used, the candidates are queried from the LSH indexes and unioned
        // Filter the union to only candidates with similar types.
        // For each candidate in the union, Jaccard similarity is calculated, if one candidate doesn't appear
        // in one LSH index, the default Jaccard similarity is 0.
        else if (MeasureType.onlyLSH(query_measures))
            results.addAll(onlyLSHQuery(metadata, query_measures, is_join, threshold));
        // If both WordNet and LSH measures are used, first retrieve candidates from LSH indexes like
        // above, then calculate WordNet similarity among the candidates.
        else
            results.addAll(mixedQuery(metadata, query_measures, is_join, threshold));
        return results;
    }

    private QueryResults onlyWordNetQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join, double threshold) {
        QueryResults results = new QueryResults(new ArrayList<>());
        String table_id = metadata.getId().split(Constants.SEPARATOR, 2)[0];
        List<String> similar_types = Constants.typeInList(metadata.getType());

        // Candidates are only those that have similar types
        List<Metadata> candidates = metadata_repository.findByIdNotStartsWithAndTypeIn(table_id, similar_types);

        for (Metadata candidate : candidates) {
            Measures measure_list = new Measures(new ArrayList<>());
            for (int i = 0; i < query_measures.size(); i++) {
                int remaining_weight = getRemainingWeights(query_measures, i, is_join);
                if (belowThreshold(threshold, measure_list.totalWeight(), measure_list.weightedSum(), remaining_weight)) {
                    measure_list.clear();
                    System.out.println("Below threshold, skipped");
                    break;
                }
                MeasureType measure = query_measures.get(i);
                measure_list.addMeasure(calculateWordnetMeasure(measure, metadata, candidate, is_join));
            }
            if (!measure_list.isEmpty())
                results.add(metadata, candidate, measure_list.weightedAverage());
        }
        return results;
    }

    private QueryResults onlyLSHQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join, double threshold) {
        QueryResults results = new QueryResults(new ArrayList<>());
        List<String> similar_types = Constants.typeInList(metadata.getType());

        Map<MeasureType, Map<Metadata, Score>> candidates = mapMeasureTypeToCandidates(metadata, query_measures);

        Set<Metadata> candidate_set = unionCandidates(candidates, similar_types);

        for (Metadata candidate : candidate_set) {
            Measures measure_list = new Measures(new ArrayList<>());
            for (int i = 0; i < query_measures.size(); i++) {
                int remaining_weight = getRemainingWeights(query_measures, i, is_join);
                if (belowThreshold(threshold, measure_list.totalWeight(), measure_list.weightedSum(), remaining_weight)) {
                    measure_list.clear();
                    System.out.println("Below threshold, skipped");
                    break;
                }
                MeasureType measure = query_measures.get(i);
                measure_list.addMeasure(calculateLSHMeasure(measure, candidate, is_join, candidates));
            }
            if (!measure_list.isEmpty())
                results.add(metadata, candidate, measure_list.weightedAverage());
        }
        return results;
    }

    private QueryResults mixedQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join, double threshold) {
        QueryResults results = new QueryResults(new ArrayList<>());
        List<String> similar_types = Constants.typeInList(metadata.getType());

        Map<MeasureType, Map<Metadata, Score>> candidates = mapMeasureTypeToCandidates(metadata, query_measures);
        Set<Metadata> candidate_set = unionCandidates(candidates, similar_types);

        for (Metadata candidate : candidate_set) {
            Measures measure_list = new Measures(new ArrayList<>());
            for (int i = 0; i < query_measures.size(); i++) {
                int remaining_weight = getRemainingWeights(query_measures, i, is_join);
                if (belowThreshold(threshold, measure_list.totalWeight(), measure_list.weightedSum(), remaining_weight)) {
                    measure_list.clear();
                    System.out.println("Below threshold, skipped");
                    break;
                }
                MeasureType measure = query_measures.get(i);
                if (MeasureType.isLSH(measure))
                    measure_list.addMeasure(calculateLSHMeasure(measure, candidate, is_join, candidates));
                else
                    measure_list.addMeasure(calculateWordnetMeasure(measure, metadata, candidate, is_join));
            }
            if (!measure_list.isEmpty())
                results.add(metadata, candidate, measure_list.weightedAverage());
        }
        return results;
    }

    private boolean belowThreshold(double threshold, int total_weight, double weighted_sum, int remaining_weight) {
        return remaining_weight < threshold * total_weight - weighted_sum;

    }

    private int getRemainingWeights(List<MeasureType> query_measures, int current_iteration , boolean is_join) {
        int remaining_weights = 0;
        for (int i = current_iteration; i < query_measures.size(); i++) {
            remaining_weights += MeasureType.getWeight(query_measures.get(i), is_join);
        }
        return remaining_weights;
    }

    /**
     * Map each type of measure to its corresponding candidate map
     */
    private Map<MeasureType, Map<Metadata, Score>> mapMeasureTypeToCandidates(Metadata metadata, List<MeasureType> query_measures) {
        Map<MeasureType, Map<Metadata, Score>> candidates = new HashMap<>();
        for (MeasureType measure : query_measures) {
            switch (measure) {
                case TABLE_NAME_QGRAM -> candidates.put(measure, lsh_index.queryTableName(metadata));
                case COLUMN_NAME_QGRAM -> candidates.put(measure, lsh_index.queryColumnName(metadata));
                case COLUMN_VALUE -> candidates.put(measure, lsh_index.queryColumnValue(metadata));
                case COLUMN_FORMAT -> candidates.put(measure, lsh_index.queryFormat(metadata));
            }
        }
        return candidates;
    }

    /**
     * Union all the candidates from each measure type into a set
     */
    private Set<Metadata> unionCandidates(Map<MeasureType, Map<Metadata, Score>> candidates, List<String> similar_types) {
        Set<Metadata> candidate_set = new HashSet<>();
        for (Map<Metadata, Score> candidate_map : candidates.values()) {
            for (Metadata m : candidate_map.keySet()) {
                if (similar_types.contains(m.getType()))
                    candidate_set.add(m);
            }
        }
        return candidate_set;
    }

    private Measure calculateWordnetMeasure(MeasureType measure, Metadata metadata, Metadata candidate, boolean is_join) {
        Measure m;
        switch (measure) {
            case TABLE_NAME_WORDNET -> {
                double sim = similarity_calculator.sentenceSimilarity(metadata.getTableName(), candidate.getTableName());
                m = new Measure(measure, sim, MeasureType.getWeight(measure, is_join));
            }
            case COLUMN_NAME_WORDNET -> {
                double sim = similarity_calculator.sentenceSimilarity(metadata.getColumnName(), candidate.getColumnName());
                m = new Measure(measure, sim, MeasureType.getWeight(measure, is_join));
            }
            default -> throw new RuntimeException("MeasureType not accepted");
        }
        return m;
    }

    private Measure calculateLSHMeasure(MeasureType measure, Metadata candidate, boolean is_join, Map<MeasureType, Map<Metadata, Score>> candidates) {
        // For each candidate in the union set, Jaccard similarity is calculated,
        // if one candidate doesn't appear in one LSH index, the default Jaccard similarity is 0.
        // For column value similarity, use Jaccard containment instead if query mode is join
        Score score = candidates.get(measure).getOrDefault(candidate, new Score(0, 0, 0));
        double sim = score.js();
        if (measure.equals(MeasureType.COLUMN_VALUE) && is_join)
            sim = score.jcx();
        return new Measure(measure, sim, MeasureType.getWeight(measure, is_join));
    }
}
