package rptu.thesis.npham.dsserver.service;

import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.dscommon.model.sketch.SketchType;
import rptu.thesis.npham.dscommon.utils.MethodTimer;
import rptu.thesis.npham.dsserver.exceptions.MetadataNotFoundException;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.sketch.Sketch;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;
import rptu.thesis.npham.dsserver.repository.MetadataRepo;
import rptu.thesis.npham.dsserver.repository.SketchesRepo;
import rptu.thesis.npham.dsserver.utils.Score;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSHIndex service class. Estimates Jaccard coefficient between two datasets. See <a href="https://doi.org/10.1109/ICDE.2019.00109">Lazo: A Cardinality-Based Method for Coupled Estimation of Jaccard Similarity and Containment</a>
 */
@Service
public class LSHIndex {
    private static final int N_PERMUTATIONS = 128;
    private static final float THRESHOLD = 0.7f;
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock(false);

    private LazoIndex table_name_index;
    private LazoIndex column_name_index;
    private LazoIndex column_value_index;
    private LazoIndex format_index;

    private final MetadataRepo metadata_repository;
    private final SketchesRepo sketches_repository;

    /**
     * Instantiates a new LSHIndex service and loads indexes from the database.
     */
    @Autowired
    public LSHIndex(MetadataRepo metadata_repository, SketchesRepo sketches_repository) {
        this.metadata_repository = metadata_repository;
        this.sketches_repository = sketches_repository;
        clearIndexes();
        MethodTimer timer = new MethodTimer("load indexes");
        timer.start();
        loadIndexes();
        timer.printElapsed();
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
     * Recreates the MinHash sketch from the hash values and cardinality in DB.
     */
    public LazoSketch recreateSketch(long cardinality, long[] hash_values) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS, lazo.sketch.SketchType.MINHASH_OPTIMAL);
        sketch.setCardinality(cardinality);
        sketch.setHashValues(hash_values);
        return sketch;
    }

    /**
     * Adds a sketch to the index.
     * @param id used to identify the sketch in a query (e.g. column ID).
     */
    public void addSketchToIndex(String id, Sketch sketch) {
        LazoSketch lazo_sketch = recreateSketch(sketch.cardinality(), sketch.hash_values());
        LOCK.writeLock().lock();
        try {
            switch (sketch.type()) {
                case TABLE_NAME -> table_name_index.update(id, lazo_sketch);
                case COLUMN_NAME -> column_name_index.update(id, lazo_sketch);
                case COLUMN_VALUE -> column_value_index.update(id, lazo_sketch);
                case FORMAT -> format_index.update(id, lazo_sketch);
                default -> throw new RuntimeException("Invalid sketch type");
            }
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    /**
     * Removes all sketches from the indexes.
     */
    public void clearIndexes() {
        LOCK.writeLock().lock();
        try {
            table_name_index = new LazoIndex(N_PERMUTATIONS);
            column_name_index = new LazoIndex(N_PERMUTATIONS);
            column_value_index = new LazoIndex(N_PERMUTATIONS);
            format_index = new LazoIndex(N_PERMUTATIONS);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    /**
     * Removes a sketch from the indexes.
     */
    public void removeSketchFromIndex(String id) {
        LOCK.writeLock().lock();
        try {
            table_name_index.remove(id);
            column_name_index.remove(id);
            column_value_index.remove(id);
            format_index.remove(id);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    /**
     * Queries the table name index for the Jaccard coefficient between two datasets.
     * @param query the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Score> queryTableName(Metadata query) {
        Sketch sketch = findSketch(query, SketchType.TABLE_NAME);
        LazoSketch lazo_sketch = recreateSketch(sketch.cardinality(), sketch.hash_values());

        return queryIndex(table_name_index, lazo_sketch, query.getId());
    }

    /**
     * Queries the column name index for the Jaccard coefficient between two datasets.
     * @param query the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Score> queryColumnName(Metadata query) {
        Sketch sketch = findSketch(query, SketchType.COLUMN_NAME);
        LazoSketch lazo_sketch = recreateSketch(sketch.cardinality(), sketch.hash_values());

        return queryIndex(column_name_index, lazo_sketch, query.getId());
    }

    /**
     * Queries the column value index for the Jaccard coefficient between two columns.
     * @param query the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Score> queryColumnValue(Metadata query) {
        Sketch sketch = findSketch(query, SketchType.COLUMN_VALUE);
        LazoSketch lazo_sketch = recreateSketch(sketch.cardinality(), sketch.hash_values());

        return queryIndex(column_value_index, lazo_sketch, query.getId());
    }

    /**
     * Queries the format index for the Jaccard coefficient between two columns.
     * @param query the metadata of the dataset to query.
     * @return a map of the candidates and their corresponding Jaccard coefficient.
     */
    public Map<Metadata, Score> queryFormat(Metadata query) {
        Sketch sketch = findSketch(query, SketchType.FORMAT);
        LazoSketch lazo_sketch = recreateSketch(sketch.cardinality(), sketch.hash_values());

        return queryIndex(format_index, lazo_sketch, query.getId());
    }

    private Sketch findSketch(Metadata metadata, SketchType sketch_type) {
        Optional<Sketches> sketches = sketches_repository.findById(metadata.getId());
        if (sketches.isEmpty()) throw new RuntimeException("No ID found");

        return sketches.get().getSketches().stream()
                .filter(s -> s.type().equals(sketch_type))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No sketch of type " + sketch_type + " found"));
    }

    private Map<Metadata, Score> queryIndex(LazoIndex index, LazoSketch sketch, String query_id) {
        Map<Metadata, Score> result = new HashMap<>();
        Set<LazoCandidate> candidates;

        String query_id_table = query_id.split(Constants.SEPARATOR, 2)[0];

        LOCK.readLock().lock();
        try {
            candidates = index.query(sketch, THRESHOLD, THRESHOLD);
        } finally {
            LOCK.readLock().unlock();
        }

        for (LazoCandidate candidate: candidates) {
            String id = (String) candidate.key;
            String id_table = id.split(Constants.SEPARATOR, 2)[0];
            if (id_table.equals(query_id_table)) continue;

            Optional<Metadata> query = metadata_repository.findById(id);
            if (query.isEmpty()) throw new MetadataNotFoundException();
            Metadata metadata = query.get();

            Score score = new Score(candidate.js, candidate.jcx, candidate.jcy);
            result.put(metadata, score);
        }

        return result;
    }
}
