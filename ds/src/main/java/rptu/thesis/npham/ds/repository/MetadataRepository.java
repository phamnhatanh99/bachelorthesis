package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;
import rptu.thesis.npham.ds.model.Metadata;

import java.security.SecureRandom;
import java.util.List;

@Repository
public interface MetadataRepository extends MongoRepository<Metadata, String> {

    public List<Metadata> findByType(String types);
    public List<Metadata> findByTypeIn(List<String> types);
}
