package rptu.thesis.npham.ds.model.ground_truth;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Objects;

@Document(collection = "ground_truth")
@CompoundIndex(def = "{'sourceTableName': 1, 'sourceColumnName': 1, 'targetTableName': 1, 'targetColumnName': 1}", unique = true)
public class GroundTruth {

    @Id
    private String id;
    @Field
    private String sourceTableName;
    @Field
    private String sourceColumnName;
    @Field
    private String targetTableName;
    @Field
    private String targetColumnName;


    public GroundTruth() {}

    public GroundTruth(String sourceTableName, String sourceColumnName, String targetTableName, String targetColumnName) {
        this.sourceTableName = sourceTableName;
        this.sourceColumnName = sourceColumnName;
        this.targetTableName = targetTableName;
        this.targetColumnName = targetColumnName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetColumnName() {
        return targetColumnName;
    }

    public void setTargetColumnName(String targetColumnName) {
        this.targetColumnName = targetColumnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroundTruth that = (GroundTruth) o;
        return sourceTableName.equals(that.sourceTableName) && sourceColumnName.equals(that.sourceColumnName) && targetTableName.equals(that.targetTableName) && targetColumnName.equals(that.targetColumnName) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTableName, sourceColumnName, targetTableName, targetColumnName);
    }

    @Override
    public String toString() {
        return sourceTableName + " - " + sourceColumnName + " -> " + targetTableName + " - " + targetColumnName;
    }
}
