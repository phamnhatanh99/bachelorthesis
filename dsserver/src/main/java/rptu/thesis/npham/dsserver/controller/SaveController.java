package rptu.thesis.npham.dsserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.*;
import rptu.thesis.npham.dscommon.model.dto.RequestObject;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dsserver.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.service.LSHIndex;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@RestController
public class SaveController {

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;
    private final LSHIndex lsh_index;

    @Autowired
    public SaveController(MetadataRepo metadata_repository, SketchesRepo sketches_repository, LSHIndex lsh_index) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        this.lsh_index = lsh_index;
    }

    @PostMapping("/save")
    public void save(@RequestBody List<RequestObject> request_objects) {
        for (RequestObject request_object : request_objects) {
            try {
                metadata_repository.save(request_object.metadata());
            } catch (DuplicateKeyException e) {
                String table_name = request_object.metadata().getTableName();
                String column_name = request_object.metadata().getColumnName();
                String type = request_object.metadata().getType();
                int size = request_object.metadata().getSize();
                int arity = request_object.metadata().getArity();
                Set<String> addresses = request_object.metadata().getAddresses();
                Optional<Metadata> metadata_o = metadata_repository.findByUniqueIndex(table_name, column_name, type, size, arity);
                Metadata metadata = metadata_o.orElseThrow(MetadataNotFoundException::new);
                metadata.getAddresses().addAll(addresses);
                continue;
            }
            sketches_repository.save(request_object.sketches());
            lsh_index.updateIndex(request_object.sketches());
        }
    }

    @GetMapping("clear")
    public String clear() {
        metadata_repository.deleteAll();
        sketches_repository.deleteAll();
        lsh_index.clearIndexes();
        return "Clear method finished";
    }

    @GetMapping("delete/{id}")
    public String delete(@PathVariable String id) {
        metadata_repository.deleteById(id);
        sketches_repository.deleteById(id);
        lsh_index.removeSketchFromIndex(id);
        return "Deleted " + id;
    }
}
