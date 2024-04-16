package rptu.thesis.npham.ds.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "metadata")
public class Metadata {

    @Id
    private String id;
    @Field
    private String table_name;
    @Field
    private String column_name;
    @Field
    private String description;
    @Field
    private String type;
    @Field
    private long cardinality;
    @Field
    private long[] sketch;
    @Field
    private String source;

    public Metadata() {

    }

    public Metadata(String table_name, String column_name, String description, String type, long cardinality, long[] sketch, String source) {
        this.table_name = table_name;
        this.column_name = column_name;
        this.description = description;
        this.type = type;
        this.cardinality = cardinality;
        this.sketch = sketch;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    public long[] getSketch() {
        return sketch;
    }

    public void setSketch(long[] sketch) {
        this.sketch = sketch;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "Metadata [" +
                "id='" + id + '\'' +
                ", table_name='" + table_name + '\'' +
                ", column_name='" + column_name + '\'' +
                ", type='" + type + '\'' +
                ", cardinality=" + cardinality +
                ", sketch=" + (sketch != null ? "Present" : "Empty") +
                ']';
    }
}
