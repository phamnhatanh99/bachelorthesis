package rptu.thesis.npham.ds.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.exceptions.NoScoreException;
import rptu.thesis.npham.ds.model.metadata.Metadata;
import rptu.thesis.npham.ds.model.QueryResults;
import rptu.thesis.npham.ds.model.similarity.SimilarityScores;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SimilarityScoresRepo;

import java.util.*;

@RestController
public class QueryController {

    private final MetadataRepo metadata_repository;
    private final SimilarityScoresRepo similarity_scores_repository;

    @Autowired
    public QueryController(MetadataRepo metadata_repository, SimilarityScoresRepo similarity_scores_repository) {
        this.metadata_repository = metadata_repository;
        this.similarity_scores_repository = similarity_scores_repository;
    }

    @GetMapping("id2/{id}")
    public QueryResults queryByID2(@PathVariable String id) {
        Optional<SimilarityScores> query = similarity_scores_repository.findById(id);
        if (query.isEmpty()) throw new NoScoreException();
        SimilarityScores scores = query.get();
        Metadata metadata = metadata_repository.findById(id).get();

        QueryResults results = new QueryResults(new ArrayList<>());

        scores.getScoreMap().entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .filter(e -> e.getValue().average() > 0.3)
                .forEach(e -> {
                    Metadata m = metadata_repository.findById(e.getKey()).get();
                    double avg = e.getValue().average();
                    results.add(metadata.toString(), m.toString(), avg);
                });

        return results;
    }

    @GetMapping("table_id/{uuid}")
    public QueryResults queryTable(@PathVariable String uuid, @RequestParam(required = false) String table_view) {
        List<Metadata> columns = metadata_repository.findByIdStartsWith(uuid);
        if (columns.isEmpty()) throw new RuntimeException("Table not found");

        QueryResults results = new QueryResults(new ArrayList<>());

        for (Metadata metadata : columns) {
            try {
                results.addAll(queryByID2(metadata.getId()).getResults());
            } catch (NoScoreException e) {
                System.out.println("No score for " + metadata.getTableName() + " - " + metadata.getColumnName());
            }
        }

        results.sortResults();
        System.out.println(results);
        return results;
    }
}
