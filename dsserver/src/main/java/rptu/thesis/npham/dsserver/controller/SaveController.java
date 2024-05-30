package rptu.thesis.npham.dsserver.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import rptu.thesis.npham.dscommon.model.dto.RequestObject;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.service.LSHIndex;

import java.util.List;

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
            metadata_repository.save(request_object.metadata());
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
