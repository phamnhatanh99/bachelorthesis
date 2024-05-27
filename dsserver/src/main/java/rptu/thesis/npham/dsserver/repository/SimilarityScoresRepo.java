package rptu.thesis.npham.dsserver.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.dsserver.model.similarity.SimilarityScores;

@Repository
public interface SimilarityScoresRepo extends MongoRepository<SimilarityScores, String> {
}
