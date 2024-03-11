/**
 * A simple record to hold two string values.
 */
public record Pair(String table, String column) {
    @Override
    public String toString() {
        return table + "." + column;
    }

}
