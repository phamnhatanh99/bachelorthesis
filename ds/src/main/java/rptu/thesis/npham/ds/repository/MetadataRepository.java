package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.Metadata;

import java.util.List;

@Repository
public interface MetadataRepository extends MongoRepository<Metadata, String> {

    List<Metadata> findByType(String types);
    List<Metadata> findByTypeIn(List<String> types);
}
