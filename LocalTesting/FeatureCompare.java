import lazo.index.LazoIndex;
import lazo.index.LazoIndex.LazoCandidate;
import lazo.sketch.LazoSketch;
import org.postgresql.util.PSQLException;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class FeatureCompare {
    private final DatabaseObject database_object;
    private final Map<Pair, LazoSketch> sketchMap = new HashMap<>();
    private final LazoIndex intIndex = new LazoIndex();
    private final LazoIndex doubleIndex = new LazoIndex();
    private final LazoIndex stringIndex = new LazoIndex();


    public FeatureCompare(String url, String user, String password) throws SQLException {
        database_object = new DatabaseObject(url, user, password);
        for (String table : database_object.listTables()) {
            if (table.equals("lookup_prediction")) continue;
            ResultSet columns = database_object.getColumns(table);
            while (columns.next()) {
                String column = columns.getString(4);
                Pair pair = new Pair(table, column);
                int type = database_object.getColumnDataType(pair);
                if (DatabaseObject.isInteger(type)) {
                    LazoSketch sketch = new LazoSketch();
                    ResultSet values = database_object.getColumnData(pair);
                    while (values.next()) {
                        int value;
                        try {
                            value = values.getInt(1);
                        }
                        catch (PSQLException e) {
                            value = values.getBoolean(1) ? 1 : 0;
                        }
                        if (!values.wasNull())
                            sketch.update(String.valueOf(value));
                    }
                    intIndex.insert(pair, sketch);
                    sketchMap.put(pair, sketch);
                }
                if (DatabaseObject.isDouble(type)) {
                    LazoSketch sketch = new LazoSketch();
                    ResultSet values = database_object.getColumnData(pair);
                    while (values.next()) {
                        int value = (int) values.getDouble(1);
                        if (!values.wasNull())
                            sketch.update(String.valueOf(value));
                    }
                    doubleIndex.insert(pair, sketch);
                    sketchMap.put(pair, sketch);
                }
                if (DatabaseObject.isString(type)) {
                    LazoSketch sketch = new LazoSketch();
                    ResultSet values = database_object.getColumnData(pair);
                    while (values.next()) {
                        String value = values.getString(1);
                        if (!values.wasNull())
                            sketch.update(value);
                    }
                    stringIndex.insert(pair, sketch);
                    sketchMap.put(pair, sketch);
                }
            }
        }
    }

    public double tableName(Pair source, Pair target) {
        return stringSimilarity(source.table(), target.table());
    }

    public double attributeName(Pair source, Pair target) {
        return stringSimilarity(source.column(), target.column());
    }

    private double stringSimilarity(String s1, String s2) {
        double levenshtein = 1 - levenshteinDistance(s1, s2);
        double jaccard = jaccardShingling(s1, s2);
        return (levenshtein + jaccard) / 2;
    }

    private double levenshteinDistance(String string1, String string2) {
        String s1 = string1.toLowerCase();
        String s2 = string2.toLowerCase();
        int[][] matrix = new int[s1.length() + 1][s2.length() + 1];

        // First row and column of the matrix
        for (int i = 0; i <= s1.length(); i++) matrix[i][0] = i;
        for (int j = 0; j <= s2.length(); j++) matrix[0][j] = j;

        // Fill in the rest of the matrix
        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
                matrix[i][j] = Math.min(Math.min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1), matrix[i - 1][j - 1] + cost);
            }
        }
        // Levenshtein distance = bottom-right cell of the matrix
        return (double) matrix[s1.length()][s2.length()] / Math.max(s1.length(), s2.length());
    }

    private double jaccardShingling(String s1, String s2) {
        int k = 3;
        Set<String> shingles1 = new HashSet<>();
        Set<String> shingles2 = new HashSet<>();
        for (int i = 0; i < s1.length() - k + 1; i++) {
            shingles1.add(s1.substring(i, i + k));
        }
        for (int i = 0; i < s2.length() - k + 1; i++) {
            shingles2.add(s2.substring(i, i + k));
        }
        Set<String> intersection = new HashSet<>(shingles1);
        intersection.retainAll(shingles2);
        return (double) intersection.size() / (shingles1.size() + shingles2.size() - intersection.size());
    }

    public double avgWidth(Pair source, Pair target) throws SQLException {
        ResultSet source_avg_width_result = database_object.queryPgStatsColumn(source, "avg_width");
        ResultSet target_avg_width_result = database_object.queryPgStatsColumn(target, "avg_width");
        double source_avg_width = 0;
        double target_avg_width = 0;
        if (source_avg_width_result.next())
            source_avg_width = source_avg_width_result.getDouble(1);
        if (target_avg_width_result.next())
            target_avg_width = target_avg_width_result.getDouble(1);
        if (source_avg_width == 0 || target_avg_width == 0) return 0.0;
        double result = source_avg_width / target_avg_width;
        return result <= 1 ? result : 1 / result;
    }

    public double mostCommonVals(Pair source, Pair target) throws SQLException {
        int sourceType = database_object.getColumnDataType(source);
        int targetType = database_object.getColumnDataType(target);
        // If the data type of one of them is not numeric, return 0.0
        if (sourceType != targetType)
            return 0.0;
        ResultSet source_most_common_vals_result = database_object.queryPgStatsColumn(source, "most_common_vals");
        ResultSet target_most_common_vals_result = database_object.queryPgStatsColumn(target, "most_common_vals");
        Array source_most_common_vals = null;
        Array target_most_common_vals = null;
        if (source_most_common_vals_result.next())
            source_most_common_vals = source_most_common_vals_result.getArray(1);
        if (target_most_common_vals_result.next())
            target_most_common_vals = target_most_common_vals_result.getArray(1);
        String[] source_most_common_vals_list;
        String[] target_most_common_vals_list;
        try {
            source_most_common_vals_list = source_most_common_vals.toString().replaceAll("[{}]", "").split(",");
            target_most_common_vals_list = target_most_common_vals.toString().replaceAll("[{}]", "").split(",");
        } catch (NullPointerException e) {
            return 0.0;
        }
        Set<String> source_set = new HashSet<>(List.of(source_most_common_vals_list));
        Set<String> target_set = new HashSet<>(List.of(target_most_common_vals_list));
        Set<String> intersection = new TreeSet<>(source_set);
        intersection.retainAll(target_set);
        return (double) intersection.size() / (source_set.size() +  target_set.size() - intersection.size());
    }

    public double jaccardSimilarity(Pair source, Pair target) throws SQLException {
        int sourceType = database_object.getColumnDataType(source);
        int targetType = database_object.getColumnDataType(target);
        if (DatabaseObject.isDateOrTime(sourceType) || DatabaseObject.isDateOrTime(targetType))
            return 0.0;
        Set<LazoCandidate> candidates;
        if (DatabaseObject.isInteger(sourceType)) candidates = query(source, intIndex);
        else if (DatabaseObject.isDouble(sourceType)) candidates = query(source, doubleIndex);
        else if (DatabaseObject.isString(sourceType)) candidates = query(source, stringIndex);
        else return 0.0;
        Set<LazoCandidate> candidate = candidates.stream().filter(c -> c.key.equals(target)).collect(Collectors.toSet());
        if (candidate.isEmpty()) return 0.0;
        return candidate.iterator().next().js;

    }

    private Set<LazoCandidate> query(Pair query, LazoIndex index) {
        return index.query(sketchMap.get(query), 0f, 0f);
    }
}
