package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.Sketches;

@Repository
public interface SketchesRepo extends MongoRepository<Sketches, String> {
}
