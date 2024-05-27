package rptu.thesis.npham.dscommon.model.sketch;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Set;

@Document(collection = "sketches")
public class Sketches {

    @Id
    private String id;
    @Field
    private Set<Sketch> sketches;

    public Sketches() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Set<Sketch> getSketches() {
        return sketches;
    }

    public void setSketches(Set<Sketch> sketches) {
        this.sketches = sketches;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sketches s)) return false;
        return id.equals(s.getId()) && sketches.equals(s.getSketches());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
