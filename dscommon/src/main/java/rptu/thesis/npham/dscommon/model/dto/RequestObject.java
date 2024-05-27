package rptu.thesis.npham.dscommon.model.dto;

import rptu.thesis.npham.dscommon.model.metadata.Metadata;
import rptu.thesis.npham.dscommon.model.sketch.Sketches;

public record RequestObject(Metadata metadata, Sketches sketches) {
}
