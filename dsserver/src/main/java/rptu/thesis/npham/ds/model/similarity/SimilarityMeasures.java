package rptu.thesis.npham.ds.model.similarity;

import java.util.Collection;

public enum SimilarityMeasures {
    TABLE_NAME_WORDNET, TABLE_NAME_SHINGLE, COLUMN_NAME_WORDNET, COLUMN_NAME_SHINGLE, COLUMN_VALUE, COLUMN_FORMAT;

    private static double TABLE_NAME_JOIN_WEIGHT = 1;
    private static double COLUMN_NAME_JOIN_WEIGHT = 1;
    private static double COLUMN_VALUES_JOIN_WEIGHT = 1;
    private static double COLUMN_FORMAT_JOIN_WEIGHT = 1;

    private static double TABLE_NAME_UNION_WEIGHT = 1;
    private static double COLUMN_NAME_UNION_WEIGHT = 1;
    private static double COLUMN_VALUES_UNION_WEIGHT = 1;
    private static double COLUMN_FORMAT_UNION_WEIGHT = 1;

    public static boolean onlyWordNet(Collection<SimilarityMeasures> measures) {
        return measures.stream().allMatch(m -> m == TABLE_NAME_WORDNET || m == COLUMN_NAME_WORDNET);
    }

    public static boolean onlyLSH(Collection<SimilarityMeasures> measures) {
        return measures.stream().allMatch(m -> m == TABLE_NAME_SHINGLE || m == COLUMN_NAME_SHINGLE || m == COLUMN_VALUE || m == COLUMN_FORMAT);
    }

    public static boolean isLSH(SimilarityMeasures measure) {
        return measure == TABLE_NAME_SHINGLE || measure == COLUMN_NAME_SHINGLE || measure == COLUMN_VALUE || measure == COLUMN_FORMAT;
    }

    public static double getJoinWeight(SimilarityMeasures measure) {
        return switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_SHINGLE -> TABLE_NAME_JOIN_WEIGHT;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_SHINGLE -> COLUMN_NAME_JOIN_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT;
        };
    }

    public static double getUnionWeight(SimilarityMeasures measure) {
        return switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_SHINGLE -> TABLE_NAME_UNION_WEIGHT;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_SHINGLE -> COLUMN_NAME_UNION_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT;
        };
    }

    public static void setJoinWeight(SimilarityMeasures measure, double weight) {
        switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_SHINGLE -> TABLE_NAME_JOIN_WEIGHT = weight;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_SHINGLE -> COLUMN_NAME_JOIN_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT = weight;
        }
    }

    public static void setUnionWeight(SimilarityMeasures measure, double weight) {
        switch (measure) {
            case TABLE_NAME_WORDNET, TABLE_NAME_SHINGLE -> TABLE_NAME_UNION_WEIGHT = weight;
            case COLUMN_NAME_WORDNET, COLUMN_NAME_SHINGLE -> COLUMN_NAME_UNION_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT = weight;
        }
    }
}
