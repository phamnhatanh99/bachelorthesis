package rptu.thesis.npham.dsserver.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;

@Repository
public interface SketchesRepo extends MongoRepository<Sketches, String> {
    void deleteByIdStartsWith(String regex);
}
