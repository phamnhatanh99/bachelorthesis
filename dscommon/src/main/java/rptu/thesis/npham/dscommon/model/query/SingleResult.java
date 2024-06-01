package rptu.thesis.npham.dscommon.model.query;

import rptu.thesis.npham.dscommon.model.metadata.Metadata;

public record SingleResult(Metadata query, Metadata candidate, double score) {
    @Override
    public String toString() {
        return query + " is similar to " + candidate + " with a score of " + score;
    }
}
