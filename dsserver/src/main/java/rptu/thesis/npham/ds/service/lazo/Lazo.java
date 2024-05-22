package rptu.thesis.npham.ds.service.lazo;

import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.metadata.Metadata;
import rptu.thesis.npham.ds.model.sketch.Sketch;
import rptu.thesis.npham.ds.model.sketch.Sketches;
import rptu.thesis.npham.ds.repository.MetadataRepo;
import rptu.thesis.npham.ds.repository.SketchesRepo;
import rptu.thesis.npham.ds.utils.Jaccard;
import rptu.thesis.npham.ds.utils.StringUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lazo service class. Estimates Jaccard coefficient between two datasets. See <a href="https://doi.org/10.1109/ICDE.2019.00109">Lazo: A Cardinality-Based Method for Coupled Estimation of Jaccard Similarity and Containment</a>
 */
@Service
public class Lazo {
    public static final List<String> STRINGY_TYPES = new ArrayList<>(Arrays.asList("TEXT", "STRING"));
    public static final List<String> NUMERIC_TYPES = new ArrayList<>(Arrays.asList("INTEGER", "BOOLEAN", "DOUBLE", "LONG", "FLOAT", "SHORT"));
    public static final List<String> TEMPORAL_TYPES = new ArrayList<>(Arrays.asList("LOCAL_DATE", "LOCAL_DATE_TIME", "LOCAL_TIME"));

    private static final int N_PERMUTATIONS = 256;
    private static final float THRESHOLD = 0.0f;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false);

    private LazoIndex string_index;
    private LazoIndex numeric_index;
    private LazoIndex temporal_index;
    private LazoIndex format_index;

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;

    /**
     * Instantiates a new Lazo service and loads indexes from the database.
     */
    @Autowired
    public Lazo(MetadataRepo metadata_repository, SketchesRepo sketches_repository) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        createIndexes();
        loadIndexes();
    }

    private void createIndexes() {
        lock.writeLock().lock();
        try {
            this.string_index = new LazoIndex(N_PERMUTATIONS);
            this.numeric_index = new LazoIndex(N_PERMUTATIONS);
            this.temporal_index = new LazoIndex(N_PERMUTATIONS);
            this.format_index = new LazoIndex(N_PERMUTATIONS);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void loadIndexes() {
        List<Sketches> all_sketches = sketches_repository.findAll();
        for (Sketches sketches: all_sketches) updateIndex(sketches);
    }

    /**
     * Updates the indexes a list of sketches.
     */
    public void updateIndex(Sketches sketches) {
        for (Sketch sketch: sketches.getSketches()) {
            addSketchToIndex(sketches.getId(), sketch);
        }
    }

    /**
     * Creates a new MinHash sketch from an iterable (e.g. a column).
     */
    public LazoSketch createSketch(Iterable<?> iterable) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS, lazo.sketch.SketchType.MINHASH);
        for (Object value: iterable)
            sketch.update(value.toString());
        return sketch;
    }

    /**
     * Recreates the MinHash sketch from the hash values and cardinality in DB.
     */
    public LazoSketch createSketch(long cardinality, long[] hash_values) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS, lazo.sketch.SketchType.MINHASH);
        sketch.setCardinality(cardinality);
        sketch.setHashValues(hash_values);
        return sketch;
    }

    /**
     * Adds a sketch to the index.
     * @param id used to identify the sketch in a query (e.g. column ID).
     */
    public void addSketchToIndex(String id, Sketch sketch) {
        LazoSketch lazo_sketch = createSketch(sketch.getCardinality(), sketch.getHashValues());
        lock.writeLock().lock();
        try {
            switch (sketch.getType()) {
                case STRING -> string_index.update(id, lazo_sketch);
                case NUMERIC -> numeric_index.update(id, lazo_sketch);
                case TEMPORAL -> temporal_index.update(id, lazo_sketch);
                case FORMAT -> format_index.update(id, lazo_sketch);
                default -> throw new RuntimeException("Invalid sketch type");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all sketches from the indexes.
     */
    public void clearIndexes() {
        lock.writeLock().lock();
        try {
            string_index = new LazoIndex(N_PERMUTATIONS);
            numeric_index = new LazoIndex(N_PERMUTATIONS);
            temporal_index = new LazoIndex(N_PERMUTATIONS);
            format_index = new LazoIndex(N_PERMUTATIONS);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a sketch from the indexes.
     */
    public void removeSketchFromIndex(String id) {
        lock.writeLock().lock();
        try {
            string_index.remove(id);
            numeric_index.remove(id);
            temporal_index.remove(id);
            format_index.remove(id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Queries the column value index for the Jaccard coefficient between two columns.
     * @param metadata_input the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Jaccard> queryColumnValue(Metadata metadata_input) {
        LazoIndex index;
        SketchType sketch_type;
        String type = metadata_input.getType();

        if (STRINGY_TYPES.contains(type)) {
            index = string_index;
            sketch_type = SketchType.STRING;
        }
        else if (NUMERIC_TYPES.contains(type)) {
            index = numeric_index;
            sketch_type = SketchType.NUMERIC;
        }
        else if (TEMPORAL_TYPES.contains(type)) {
            index = temporal_index;
            sketch_type = SketchType.TEMPORAL;
        }
        else throw new RuntimeException("Invalid type");

        Sketch sketch = findSketch(metadata_input, sketch_type);
        LazoSketch lazo_sketch = createSketch(sketch.getCardinality(), sketch.getHashValues());

        return queryContainment(index, lazo_sketch, metadata_input.getId());
    }

    /**
     * Queries the format index for the Jaccard coefficient between two columns.
     * @param metadata_input the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Jaccard> queryFormat(Metadata metadata_input) {
        Sketch sketch = findSketch(metadata_input, SketchType.FORMAT);
        LazoSketch lazo_sketch = createSketch(sketch.getCardinality(), sketch.getHashValues());

        return queryContainment(format_index, lazo_sketch, metadata_input.getId());
    }

    private Sketch findSketch(Metadata metadata, SketchType sketch_type) {
        Optional<Sketches> sketches = sketches_repository.findById(metadata.getId());
        if (sketches.isEmpty()) throw new RuntimeException("No ID found");

        return sketches.get().getSketches().stream()
                .filter(s -> s.getType().equals(sketch_type))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No sketch of type " + sketch_type + " found"));
    }

    private Map<Metadata, Jaccard> queryContainment(LazoIndex index, LazoSketch sketch, String query_id) {
        Map<Metadata, Jaccard> result = new HashMap<>();
        Set<LazoCandidate> candidates;

        String query_id_table = query_id.split(StringUtils.SEPARATOR, 2)[0];

        lock.readLock().lock();
        try {
            candidates = index.query(sketch, THRESHOLD, THRESHOLD);
        } finally {
            lock.readLock().unlock();
        }

        for (LazoCandidate candidate: candidates) {
            String id = (String) candidate.key;
            String id_table = id.split(StringUtils.SEPARATOR, 2)[0];
            if (id_table.equals(query_id_table)) continue;

            Optional<Metadata> query = metadata_repository.findById(id);
            if (query.isEmpty()) throw new RuntimeException("No ID found");
            Metadata metadata = query.get();

            Jaccard jaccard = new Jaccard(candidate.js, candidate.jcx, candidate.jcy);
            result.put(metadata, jaccard);
        }

        return result;
    }
}
