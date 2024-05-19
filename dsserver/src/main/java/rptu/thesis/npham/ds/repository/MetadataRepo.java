package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.metadata.Metadata;

import java.util.List;

@Repository
public interface MetadataRepo extends MongoRepository<Metadata, String> {
    List<Metadata> findByIdStartsWith(String id);
}
