package rptu.thesis.npham.dsserver.model.similarity;

import java.util.Collection;

public enum MeasureType {
    COLUMN_VALUE, COLUMN_FORMAT, COLUMN_NAME_QGRAM, TABLE_NAME_QGRAM, COLUMN_NAME_WORDNET, TABLE_NAME_WORDNET;

    private static int TABLE_NAME_JOIN_WEIGHT = 2;
    private static int COLUMN_NAME_JOIN_WEIGHT = 5;
    private static int COLUMN_VALUES_JOIN_WEIGHT = 8;
    private static int COLUMN_FORMAT_JOIN_WEIGHT = 5;

    private static int TABLE_NAME_UNION_WEIGHT = 1;
    private static int COLUMN_NAME_UNION_WEIGHT = 3;
    private static int COLUMN_VALUES_UNION_WEIGHT = 2;
    private static int COLUMN_FORMAT_UNION_WEIGHT = 4;

    /**
     * Check if the given measures are all WordNet measures
     */
    public static boolean onlyWordNet(Collection<MeasureType> measures) {
        return measures.stream().allMatch(m -> m == TABLE_NAME_WORDNET || m == COLUMN_NAME_WORDNET);
    }

    /**
     * Check if the given measures are all LSH measures
     */
    public static boolean onlyLSH(Collection<MeasureType> measures) {
        return measures.stream().allMatch(m -> m == TABLE_NAME_QGRAM || m == COLUMN_NAME_QGRAM || m == COLUMN_VALUE || m == COLUMN_FORMAT);
    }

    /**
     * Check if the given measure is a LSH measure
     */
    public static boolean isLSH(MeasureType measure) {
        return measure == TABLE_NAME_QGRAM || measure == COLUMN_NAME_QGRAM || measure == COLUMN_VALUE || measure == COLUMN_FORMAT;
    }

    public static int getWeight(MeasureType measure, boolean is_join) {
        return is_join ? getJoinWeight(measure) : getUnionWeight(measure);
    }

    public static int getJoinWeight(MeasureType measure) {
        return switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_QGRAM -> TABLE_NAME_JOIN_WEIGHT;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_QGRAM -> COLUMN_NAME_JOIN_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT;
        };
    }

    public static int getUnionWeight(MeasureType measure) {
        return switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_QGRAM -> TABLE_NAME_UNION_WEIGHT;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_QGRAM -> COLUMN_NAME_UNION_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT;
        };
    }

    public static void setWeight(MeasureType measure, int weight, boolean is_join) {
        if (is_join) setJoinWeight(measure, weight);
        else setUnionWeight(measure, weight);
    }

    public static void setJoinWeight(MeasureType measure, int weight) {
        switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_QGRAM -> TABLE_NAME_JOIN_WEIGHT = weight;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_QGRAM -> COLUMN_NAME_JOIN_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT = weight;
        }
    }

    public static void setUnionWeight(MeasureType measure, int weight) {
        switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_QGRAM -> TABLE_NAME_UNION_WEIGHT = weight;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_QGRAM -> COLUMN_NAME_UNION_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT = weight;
        }
    }
}
