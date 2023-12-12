import java.sql.SQLException;
import java.util.*;

/**
 * Retrieves similarity checks from Querier and predicts joinable columns for a given table.
 */
public class Predictor {
    private final Querier querier;
    private final GroundTruth groundTruth;

    public Predictor() throws SQLException {
        querier = new Querier();
        groundTruth = new GroundTruth();
    }

    /**
     * Predicts if the columns in the given table have any matching join partner.
     * @param table_name Name of the table to find join partners.
     */
    public void predict(String table_name) throws SQLException {
        Map<String, Set<String>> map = querier.compareColumnsByType(table_name);
        for (String name : map.keySet()) {
            predict(name, map.get(name));
        }
    }

    /**
     * Prints out possible the columns that a given column can join.
     * @param name Name of the column to predict
     * @param similar Set of possible join candidates
     */
    private void predict(String name, Set<String> similar) throws SQLException {
        Set<String> set1 = top3(querier.rankColumnByHistogramBounds(name, similar)).keySet();
        Set<String> set2 = top3(querier.rankColumnByAvgWidth(name, similar)).keySet();
        Set<String> set3 = top3(querier.rankColumnByMostCommonFreqs(name, similar)).keySet();
        Map<String, Integer> count = getCount(set1, set2, set3);
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Integer> pair: count.entrySet()) {
            if (pair.getValue() > 1)
                result.add(pair.getKey());
        }
        System.out.println("Column " + name + " can be joined with: " + result);
        groundTruth.check(name);
    }

    /**
     * Filters out a map, keeping only entries with the top 3 lowest/best values.
     * Multiples entries in the same ranking are allowed.
     * @param map The map to filter out the top 3
     * @return Filtered map with only top 3 values
     */
    private Map<String, Double> top3(Map<String, Double> map) {
        int counter = 0;
        double max = - Double.MAX_VALUE;
        Map<String, Double> result = new LinkedHashMap<>();
        for (Map.Entry<String, Double> pair : map.entrySet()) {
            if (pair.getValue() > max)
                counter++;
            if (counter > 3) break;
            result.put(pair.getKey(), pair.getValue());
            max = pair.getValue();
        }
        return result;
    }

    /**
     * Helper function, counts the occurrence of the elements in each set and store the count as value in a map.
     * @return A map from the elements in the sets to its number of appearance
     */
    private static Map<String, Integer> getCount(Set<String> set1, Set<String> set2, Set<String> set3) {
        Map<String, Integer> count = new HashMap<>();
        for (String s : set1) {
            if (count.containsKey(s))
                count.put(s, count.get(s) + 1);
            else
                count.put(s, 1);
        }
        for (String s : set2) {
            if (count.containsKey(s))
                count.put(s, count.get(s) + 1);
            else
                count.put(s, 1);
        }
        for (String s : set3) {
            if (count.containsKey(s))
                count.put(s, count.get(s) + 1);
            else
                count.put(s, 1);
        }
        return count;
    }
}
