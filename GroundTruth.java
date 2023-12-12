import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A class containing all joinable columns in the TPC-H database.
 */
public class GroundTruth {
    private final Set<Set<String>> ground_truths = new HashSet<>();

    public GroundTruth() {
        ground_truths.add(new HashSet<>(Arrays.asList("r_regionkey", "n_regionkey")));
        ground_truths.add(new HashSet<>(Arrays.asList("n_nationkey", "s_nationkey", "c_nationkey")));
        ground_truths.add(new HashSet<>(Arrays.asList("s_suppkey", "ps_suppkey", "l_suppkey")));
        ground_truths.add(new HashSet<>(Arrays.asList("p_partkey", "ps_partkey", "l_partkey")));
        ground_truths.add(new HashSet<>(Arrays.asList("c_custkey", "o_custkey")));
        ground_truths.add(new HashSet<>(Arrays.asList("o_orderkey", "l_orderkey")));
    }

    /**
     * Prints out the set of joinable columns the given column belongs to
     * @param column_name One column in a set of joinable columns
     */
    public void check(String column_name) {
        String trimmed = column_name.substring(2);
        for (Set<String> set: ground_truths) {
            for (String string: set) {
                if (string.contains(trimmed)) {
                    System.out.println("Ground truth: " + set + "\n");
                    break;
                }
            }
        }
    }
}
