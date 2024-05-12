package rptu.thesis.npham.ds.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.model.Score;
import rptu.thesis.npham.ds.model.SimilarityScores;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.service.Lazo;
import rptu.thesis.npham.ds.service.Similarity;
import rptu.thesis.npham.ds.utils.Jaccard;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
public class QueryController {

    private final MetadataRepo metadata_repository;
    private final Lazo lazo;
    private final Similarity similarity;

    @Autowired
    public QueryController(MetadataRepo metadata_repository, Lazo lazo, Similarity similarity) {
        this.metadata_repository = metadata_repository;
        this.lazo = lazo;
        this.similarity = similarity;
    }

    @GetMapping("id/{id}")
    public void queryByID(@PathVariable String id) {
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

        SimilarityScores scores = new SimilarityScores(metadata.getId(), new HashMap<>());

        for (Metadata m: containment_candidate) {
            double table_name_sim = table_name_similarity.get(m);
            double column_name_sim = column_name_similarity.get(m);
            double containment_sim = containment_similarity.get(m).jcx();
            double format_sim = format_similarity.getOrDefault(m, new Jaccard(0, 0, 0)).jcx();
            scores.getScoreMap().put(m.getId(), new Score(table_name_sim, column_name_sim, containment_sim, format_sim, 0));
        }

        scores.getScoreMap().entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .forEach(e -> {
                    Metadata m = metadata_repository.findById(e.getKey()).get();
                    String table_name_ = m.getTableName();
                    String column_name_ = m.getColumnName();
                    double avg = e.getValue().average();
                    if (avg > 0.3)
                        System.out.println(table_name_ + " - " + column_name_ + " - " + e.getValue().average());
                });
    }
}
