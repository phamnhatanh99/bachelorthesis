package rptu.thesis.npham.ds.controller;

import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.repository.MetadataRepository;
import rptu.thesis.npham.ds.service.Lazo;
import rptu.thesis.npham.ds.utils.Constants;

import java.util.Map;
import java.util.Optional;

@RestController
public class Querier {

    private final MetadataRepository metadata_repository;
    private final Lazo lazo;

    @Autowired
    public Querier(MetadataRepository metadata_repository, Lazo lazo) {
        this.metadata_repository = metadata_repository;
        this.lazo = lazo;
    }

    @GetMapping("id/{id}")
    public Map<String, Double> query(@PathVariable String id) {
        Optional<Metadata> query = metadata_repository.findById(id);
        if (query.isEmpty()) throw new RuntimeException("ID does not exists");
        Metadata metadata = query.get();
        String table_name = metadata.getTable_name();
        String type = metadata.getType();
        LazoSketch sketch = Lazo.createSketch(metadata.getCardinality(), metadata.getSketch());
        Map<String, Double> result =  lazo.querySimilarity(sketch, type);
        result.keySet().removeIf(key -> key.split(Constants.SEPARATOR, 2)[0].equals(table_name));
        return result;
    }
}
