package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.metadata.Metadata;

import java.util.List;

@Repository
public interface MetadataRepo extends MongoRepository<Metadata, String> {
    List<Metadata> findByIdStartsWith(String regex);
    List<Metadata> findByType(String type);
    List<Metadata> findByTypeIn(List<String> types);
    @Query("{ '_id': { '$not': { '$regex': '^?0' } }, 'type': { '$in': ?1 } }")
    List<Metadata> findByIdNotStartsWithAndTypeIn(String regex, List<String> types);
}
