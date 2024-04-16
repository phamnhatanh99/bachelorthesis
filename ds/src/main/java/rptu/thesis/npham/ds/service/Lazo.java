package rptu.thesis.npham.ds.service;

import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;
import lazo.sketch.SketchType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.Metadata;
import rptu.thesis.npham.ds.repository.MetadataRepository;
import rptu.thesis.npham.ds.utils.Constants;
import tech.tablesaw.columns.Column;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Lazo service class. Estimates Jaccard coefficient between two datasets. See <a href="https://doi.org/10.1109/ICDE.2019.00109">Lazo: A Cardinality-Based Method for Coupled Estimation of Jaccard Similarity and Containment</a>
 */
@Service
public class Lazo {
    private static final int N_PERMUTATIONS = 128;
    private static final float THRESHOLD = 0.1f;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false);

    private final LazoIndex string_index;
    private final LazoIndex numeric_index;

    private final MetadataRepository metadata_repository;

    @Autowired
    public Lazo(MetadataRepository metadata_repository) {
        this.metadata_repository = metadata_repository;
        lock.writeLock().lock();
        try {
            this.string_index = new LazoIndex(N_PERMUTATIONS);
            this.numeric_index = new LazoIndex(N_PERMUTATIONS);
        } finally {
            lock.writeLock().unlock();
        }
        // Retrieve all text sketches from DB
        List<Metadata> metadata_list = metadata_repository.findByTypeIn(Constants.STRINGY_TYPES);
        for (Metadata metadata: metadata_list)
            updateIndex(metadata, string_index);
        // Retrieve all numeric sketches from DB
        metadata_list = metadata_repository.findByTypeIn(Constants.NUMERIC_TYPES);
        for (Metadata metadata: metadata_list)
            updateIndex(metadata, numeric_index);
    }

    public static LazoSketch createSketch(Column<?> column) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS, SketchType.MINHASH);
        for (Object value: column)
            sketch.update(value.toString());
        return sketch;
    }

    public static LazoSketch createSketch(long cardinality, long[] hash_values) {
        LazoSketch sketch = new LazoSketch(N_PERMUTATIONS, SketchType.MINHASH);
        sketch.setCardinality(cardinality);
        sketch.setHashValues(hash_values);
        return sketch;
    }

    public void updateIndex(Metadata metadata, LazoIndex index) {
        String id = metadata.getId();
        long cardinality = metadata.getCardinality();
        long[] hash = metadata.getSketch();
        LazoSketch sketch = createSketch(cardinality, hash);
        lock.writeLock().lock();
        try {
            index.update(id, sketch);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<String, Double> querySimilarity(LazoSketch sketch, String type) {
        LazoIndex index;
        if (Constants.STRINGY_TYPES.contains(type)) index = string_index;
        else if (Constants.NUMERIC_TYPES.contains(type)) index = numeric_index;
        else throw new RuntimeException("Invalid type");
        Map<String, Double> result = new HashMap<>();
        Set<LazoCandidate> candidates;
        lock.readLock().lock();
        try {
            candidates = index.queryContainment(sketch, THRESHOLD);
        } finally {
            lock.readLock().unlock();
        }
        for (LazoCandidate candidate: candidates) {
            String id = (String) candidate.key;
            Optional<Metadata> query = metadata_repository.findById(id);
            if (query.isEmpty()) throw new RuntimeException("No ID found");
            Metadata metadata = query.get();

            result.put(metadata.getTable_name() + Constants.SEPARATOR + metadata.getColumn_name(), (double) candidate.jcx);
        }
        return result.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }

}
