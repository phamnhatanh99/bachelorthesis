package rptu.thesis.npham.ds.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "metadata")
@CompoundIndex(name = "unique_index", def="{'table_name' : 1, 'column_name' : 1, 'type' : 1, 'size': 1, 'arity': 1}", unique = true)
public class Metadata {

    @Id
    private String id;
    @Field
    private String table_name;
    @Field
    private String column_name;
    @Field
    private String type;
    @Field
    private int size;
    @Field
    private int arity;
    @Field
    private String address;

    public Metadata() {}

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getArity() {
        return arity;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return table_name + " - " + column_name + " (" + id + ")";
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
