package rptu.thesis.npham.dsserver.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;

import java.util.List;
import java.util.Optional;

@Repository
public interface MetadataRepo extends MongoRepository<Metadata, String> {
    List<Metadata> findByIdStartsWith(String regex);
    void deleteByIdStartsWith(String regex);
    @Query("{ 'table_name': ?0, 'column_name': ?1, 'type': ?2, 'size': ?3, 'arity': ?4 }")
    Optional<Metadata> findByUniqueIndex(String table_name, String column_name, String type, int size, int arity);
    @Query("{ '_id': { '$not': { '$regex': '^?0' } }, 'type': { '$in': ?1 } }")
    List<Metadata> findByIdNotStartsWithAndTypeIn(String regex, List<String> types);
}
