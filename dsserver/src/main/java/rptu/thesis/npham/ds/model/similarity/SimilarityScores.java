package rptu.thesis.npham.ds.model.similarity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Map;

@Document(collection = "similarity_scores")
public class SimilarityScores {

        @Id
        private String column_id;
        @Field
        private Map<String, MeasureList> score_map;

    public SimilarityScores(String column_id, Map<String, MeasureList> score_map) {
            this.column_id = column_id;
            this.score_map = score_map;
        }

        public String getColumnID() {
            return column_id;
        }

        public void setColumnID(String column_id) {
            this.column_id = column_id;
        }

        public Map<String, MeasureList> getScoreMap() {
            return score_map;
        }

        public void setScoreMap(Map<String, MeasureList> score_map) {
            this.score_map = score_map;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SimilarityScores s)) return false;
            return column_id.equals(s.getColumnID());
        }

        @Override
        public int hashCode() {
            return column_id.hashCode();
        }
}
