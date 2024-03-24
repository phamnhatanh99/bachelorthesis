import java.util.List;

public record Column(String table_name, String column_name, int data_type, int avg_width, List<String> most_common_vals, long[] sketch, long cardinality) {
}
