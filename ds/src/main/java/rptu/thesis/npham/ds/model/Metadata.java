package rptu.thesis.npham.ds.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

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
    private String uri;

    public Metadata() {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTableName() {
        return table_name;
    }

    public void setTableName(String table_name) {
        this.table_name = table_name;
    }

    public String getColumnName() {
        return column_name;
    }

    public void setColumnName(String column_name) {
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

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public String toString() {
        return table_name + " - " + column_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Metadata metadata)) return false;
        return this.id.equals(metadata.getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

}
