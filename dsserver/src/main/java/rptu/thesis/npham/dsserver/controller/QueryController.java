package rptu.thesis.npham.dsserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.*;
import rptu.thesis.npham.dscommon.model.dto.RequestObject;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.query.QueryResults;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.utils.MethodTimer;
import rptu.thesis.npham.dsserver.evaluation.Datasets;
import rptu.thesis.npham.dsserver.evaluation.Evaluator;
import rptu.thesis.npham.dsserver.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.dsserver.model.similarity.Measure;
import rptu.thesis.npham.dsserver.model.similarity.MeasureType;
import rptu.thesis.npham.dsserver.model.similarity.Measures;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.service.LSHIndex;
import rptu.thesis.npham.dsserver.service.SimilarityCalculator;
import rptu.thesis.npham.dsserver.utils.Jaccard;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class QueryController {

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final LSHIndex lsh_index;
    private final SimilarityCalculator similarity_calculator;
    private final Evaluator evaluator;

    @Autowired
    public QueryController(MetadataRepo metadata_repository, SketchesRepo sketches_repository, SimilarityCalculator similarity_calculator, Evaluator evaluator, LSHIndex lsh_index) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.similarity_calculator = similarity_calculator;
        this.evaluator = evaluator;
        this.lsh_index = lsh_index;
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
            List<RequestObject> request_objects = new ArrayList<>();
            for (Metadata metadata : columns) {
                Sketches sketches = sketches_repository.findById(metadata.getId()).orElseThrow(MetadataNotFoundException::new);
                request_objects.add(new RequestObject(metadata, sketches));
            }
            query(request_objects, "join", Optional.of(100), Optional.empty());
        }
    }

    /**
     * Rest end point to perform a dataset search
     * @param request_objects Wrapper object that contains the metadatas and sketches of the columns of the query table
     * @param mode Query mode, either "join" or "union"
     * @param limit The maximum amount of results to be returned
     * @param threshold The minimum score where a candidate is considered a match
     * @return The results of a query
     */
    @PostMapping("/query")
    public QueryResults query(@RequestBody List<RequestObject> request_objects,
                              @RequestParam("mode") String mode,
                              @RequestParam("limit") Optional<Integer> limit,
                              @RequestParam("threshold") Optional<Double> threshold) {
        MethodTimer timer = new MethodTimer("APIQuery");
        timer.start();

        List<MeasureType> query_measures = new ArrayList<>();
        boolean is_join = false;

        // Manually set which measures to use for a query
        if (mode.equals("join")) {
            System.out.println("Join mode");
            is_join = true;
            query_measures.add(MeasureType.TABLE_NAME_WORDNET);
            query_measures.add(MeasureType.COLUMN_NAME_WORDNET);
            query_measures.add(MeasureType.COLUMN_VALUE);
            query_measures.add(MeasureType.COLUMN_FORMAT);
        } else {
            System.out.println("Union mode");
            query_measures.add(MeasureType.COLUMN_VALUE);
            query_measures.add(MeasureType.COLUMN_FORMAT);
        }

        // Boolean to check if the table being queried already exists in DB
        boolean existed = false;
        // All metadatas in the request come from the same table, so only need to retrieve the table id from the first metadata
        Metadata first_col = request_objects.get(0).metadata();
        String table_id = first_col.getId().split(Constants.SEPARATOR, 2)[0];

        // Attempt to save the metadatas into DB for querying
        for (RequestObject request_object : request_objects) {
            try {
                metadata_repository.save(request_object.metadata());
                sketches_repository.save(request_object.sketches());
                lsh_index.updateIndex(request_object.sketches());
            } catch (DuplicateKeyException e) { // Table already exists in the database, don't save and query from the database instead
                existed = true;
                break;
            }
        }

        List<Metadata> columns;
        // Query table is not in DB, so we can retrieve the metadatas from its original uuid
        if (!existed) {
            columns = metadata_repository.findByIdStartsWith(table_id); // Can also just get the metadatas from the RequestObjects
            if (columns.isEmpty()) throw new RuntimeException("Table not found");
        }
        // Query table is in DB, but the uuid generated is different, so we query the DB using the unique index instead
        else {
            Optional<Metadata> sample_col =
                    metadata_repository.findByUniqueIndex(
                            first_col.getTableName(), first_col.getColumnName(), first_col.getType(), first_col.getSize(), first_col.getArity());
            String table_id_sample = sample_col.orElseThrow(MetadataNotFoundException::new).getId().split(Constants.SEPARATOR, 2)[0];
            columns = metadata_repository.findByIdStartsWith(table_id_sample);
        }

        QueryResults results = new QueryResults(new ArrayList<>());

        for (Metadata metadata : columns)
            results.addAll(queryUsingCertainMeasures(metadata.getId(), is_join, query_measures));

        results = results.sortResults();
        if (threshold.isPresent()) results = results.withThreshold(threshold.get());
        if (limit.isPresent()) results = results.limitResults(limit.get());
        // Remove the query from the DB once finished
        if (!existed) {
            metadata_repository.deleteByIdStartsWith(table_id);
            sketches_repository.deleteByIdStartsWith(table_id);
            request_objects.forEach(request_object -> lsh_index.removeSketchFromIndex(request_object.sketches().getId()));
        }

        timer.stop();
        evaluator.evaluate(results, Datasets.TUS_S);
        return results;
    }

    /**
     * Perform similarity calculations based on the similarity measures available
     * @param id Id of the metadata/column that needs to be queried
     * @param is_join Boolean to check if the query mode is a join or a union
     * @param query_measures The similarity measures used for the query
     * @return The query results
     */
    private QueryResults queryUsingCertainMeasures(String id, boolean is_join, List<MeasureType> query_measures) {
        QueryResults results = new QueryResults(new ArrayList<>());
        Metadata metadata = metadata_repository.findById(id).orElseThrow(MetadataNotFoundException::new);
        // If only WordNet measures are used, find columns with similar types and calculate WordNet similarity among them
        if (MeasureType.onlyWordNet(query_measures))
            results.addAll(onlyWordNetQuery(metadata, query_measures, is_join));
        // If only LSH measures are used, the candidates are queried from the LSH indexes and unioned
        // Filter the union to only candidates with similar types.
        // For each candidate in the union, Jaccard similarity is calculated, if one candidate doesn't appear
        // in one LSH index, the default Jaccard similarity is 0.
        else if (MeasureType.onlyLSH(query_measures))
            results.addAll(onlyLSHQuery(metadata, query_measures, is_join));
        // If both WordNet and LSH measures are used, first retrieve candidates from LSH indexes like
        // above, then calculate WordNet similarity among the candidates.
        else
            results.addAll(mixedQuery(metadata, query_measures, is_join));
        return results;
    }

    private QueryResults onlyWordNetQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join) {
        QueryResults results = new QueryResults(new ArrayList<>());
        String table_id = metadata.getId().split(Constants.SEPARATOR, 2)[0];
        List<String> similar_types = Constants.typeInList(metadata.getType());

        // Candidates are only those that have similar types
        List<Metadata> candidates = metadata_repository.findByIdNotStartsWithAndTypeIn(table_id, similar_types);

        for (Metadata candidate : candidates) {
            // Create the list of measures for each candidate
            Measures measure_list = new Measures(new ArrayList<>());
            // Calculate the measures according to what type of measures there are
            for (MeasureType measure : query_measures) {
                measure_list.addMeasure(calculateWordnetMeasure(measure, metadata, candidate, is_join));
            }
            results.add(metadata, candidate, measure_list.weightedAverage());
        }
        return results;
    }

    private QueryResults onlyLSHQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join) {
        QueryResults results = new QueryResults(new ArrayList<>());
        List<String> similar_types = Constants.typeInList(metadata.getType());

        Map<MeasureType, Map<Metadata, Jaccard>> candidates = mapMeasureTypeToCandidates(metadata, query_measures);

        Set<Metadata> candidate_set = unionCandidates(candidates, similar_types);

        for (Metadata candidate : candidate_set) {
            Measures measure_list = new Measures(new ArrayList<>());
            for (MeasureType measure : query_measures) {
                measure_list.addMeasure(calculateLSHMeasure(measure, candidate, is_join, candidates));
            }
            results.add(metadata, candidate, measure_list.weightedAverage());
        }
        return results;
    }

    private QueryResults mixedQuery(Metadata metadata, List<MeasureType> query_measures, boolean is_join) {
        QueryResults results = new QueryResults(new ArrayList<>());
        List<String> similar_types = Constants.typeInList(metadata.getType());

        Map<MeasureType, Map<Metadata, Jaccard>> candidates = mapMeasureTypeToCandidates(metadata, query_measures);
        Set<Metadata> candidate_set = unionCandidates(candidates, similar_types);

        for (Metadata candidate : candidate_set) {
            Measures measure_list = new Measures(new ArrayList<>());
            for (MeasureType measure : query_measures) {
                if (MeasureType.isLSH(measure))
                    measure_list.addMeasure(calculateLSHMeasure(measure, candidate, is_join, candidates));
                else
                    measure_list.addMeasure(calculateWordnetMeasure(measure, metadata, candidate, is_join));
            }
            results.add(metadata, candidate, measure_list.average());
        }
        return results;
    }

    /**
     * Map each type of measure to its corresponding candidate map
     */
    private Map<MeasureType, Map<Metadata, Jaccard>> mapMeasureTypeToCandidates(Metadata metadata, List<MeasureType> query_measures) {
        Map<MeasureType, Map<Metadata, Jaccard>> candidates = new HashMap<>();
        for (MeasureType measure : query_measures) {
            switch (measure) {
                case TABLE_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryTableName(metadata));
                case COLUMN_NAME_SHINGLE -> candidates.put(measure, lsh_index.queryColumnName(metadata));
                case COLUMN_VALUE -> candidates.put(measure, lsh_index.queryColumnValue(metadata));
                case COLUMN_FORMAT -> candidates.put(measure, lsh_index.queryFormat(metadata));
            }
        }
        return candidates;
    }

    /**
     * Union all the candidates from each measure type into a set
     */
    private Set<Metadata> unionCandidates(Map<MeasureType, Map<Metadata, Jaccard>> candidates, List<String> similar_types) {
        Set<Metadata> candidate_set = new HashSet<>();
        for (Map<Metadata, Jaccard> candidate : candidates.values()) {
            for (Metadata m : candidate.keySet()) {
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

    private Measure calculateLSHMeasure(MeasureType measure, Metadata candidate, boolean is_join, Map<MeasureType, Map<Metadata, Jaccard>> candidates) {
        // For each candidate in the union set, Jaccard similarity is calculated,
        // if one candidate doesn't appear in one LSH index, the default Jaccard similarity is 0.
        // For column value similarity, use Jaccard containment instead if query mode is join
        Jaccard jaccard = candidates.get(measure).getOrDefault(candidate, new Jaccard(0, 0, 0));
        double sim = jaccard.js();
        if (measure.equals(MeasureType.COLUMN_VALUE) && is_join)
            sim = jaccard.jcx();
        return new Measure(measure, sim, MeasureType.getWeight(measure, is_join));
    }
}
