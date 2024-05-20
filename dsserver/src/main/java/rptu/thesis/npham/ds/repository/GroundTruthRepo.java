package rptu.thesis.npham.ds.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import rptu.thesis.npham.ds.model.ground_truth.GroundTruth;

import java.util.List;
import java.util.Optional;

public interface GroundTruthRepo extends MongoRepository<GroundTruth, String> {

    Optional<GroundTruth> findBySourceTableNameAndTargetTableNameAndSourceColumnNameAndTargetColumnName(String sourceTableName, String targetTableName, String sourceColumnName, String targetColumnName);

    List<GroundTruth> findBySourceTableNameAndSourceColumnName(String sourceTableName, String sourceColumnName);
}
