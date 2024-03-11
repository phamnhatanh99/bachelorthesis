import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws SQLException {
        Instant start = Instant.now();
        DatabaseObject database_object = new DatabaseObject("jdbc:postgresql://localhost:5432/lookup_prediction", "postgres", "1234");
        Analyzer analyzer = new Analyzer("jdbc:postgresql://localhost:5432/pagila", "postgres", "1234");
        Map<Map.Entry<Pair, Pair>, List<Double>> result = analyzer.analyze();
        for (Map.Entry<Map.Entry<Pair, Pair>, List<Double>> entry : result.entrySet()) {
            try {
                database_object.insertPrediction(entry.getKey().getKey(), entry.getKey().getValue(), entry.getValue().get(0), entry.getValue().get(1), entry.getValue().get(2), entry.getValue().get(3), entry.getValue().get(4));
            } catch (PSQLException e) {
                System.out.println("Error: " + e.getMessage());
                System.out.println(entry.getKey().getKey().toString() + " - " + entry.getKey().getValue().toString() + " - " + entry.getValue().toString());
            }
        }
        Instant finish = Instant.now();
        System.out.println("Prediction table updated. Time elapsed in seconds: " + Duration.between(start, finish).toSeconds());
    }
}


