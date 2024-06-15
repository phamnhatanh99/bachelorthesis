package rptu.thesis.npham.dsserver.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import rptu.thesis.npham.dsserver.model.ground_truth.GroundTruth;

import java.util.List;
import java.util.Optional;

public interface GroundTruthRepo extends MongoRepository<GroundTruth, String> {

    List<GroundTruth> findBySourceTableName(String sourceTableName);

    Optional<GroundTruth> findBySourceTableNameAndTargetTableNameAndSourceColumnNameAndTargetColumnName(String sourceTableName, String targetTableName, String sourceColumnName, String targetColumnName);

    List<GroundTruth> findBySourceTableNameAndSourceColumnName(String sourceTableName, String sourceColumnName);
}
