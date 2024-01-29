import java.util.List;
import java.util.stream.Stream;

/**
 * An Enum containing the table names of the TPC-H database. Contains some string methods to work with the table names.
 */
public enum TPCH {
    CUSTOMER, LINEITEM, NATION, ORDERS, PART, PARTSUPP, REGION, SUPPLIER;

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    /**
     * List the table as a string.
     * @return The string listing the tables' names
     */
    public static String listTables() {
        return String.join(", ", stringList());
    }

    /**
     * Return the tables' names as list of string.
     * @return String list containing the tables' names
     */
    public static List<String> stringList() {
        return Stream.of(TPCH.values()).map(Enum::toString).toList();
    }

    /**
     * Returns a table's name using the prefixes of its column names.
     * @param column_name Name of the column in the database
     * @return The table the column belongs to
     */
    public static String getTableName (String column_name) {
        switch (column_name.substring(0, 2)) {
            case "c_" -> {
                return CUSTOMER.toString();
            }
            case "l_" -> {
                return LINEITEM.toString();
            }
            case "n_" -> {
                return NATION.toString();
            }
            case "o_" -> {
                return ORDERS.toString();
            }
            case "ps" -> {
                return PARTSUPP.toString();
            }
            case "p_" -> {
                return PART.toString();
            }
            case "r_" -> {
                return REGION.toString();
            }
            case "s_" -> {
                return SUPPLIER.toString();
            }
            default -> {
                return "";
            }
        }
    }
}
