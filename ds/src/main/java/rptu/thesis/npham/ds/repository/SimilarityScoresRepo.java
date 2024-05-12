package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.SimilarityScores;

@Repository
public interface SimilarityScoresRepo extends MongoRepository<SimilarityScores, String> {
}
