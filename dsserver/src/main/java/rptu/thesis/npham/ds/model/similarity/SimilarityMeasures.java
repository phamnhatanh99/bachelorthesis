package rptu.thesis.npham.ds.model.similarity;

public enum SimilarityMeasures {
    TABLE_NAME, TABLE_NAME_SHINGLE, COLUMN_NAME, COLUMN_NAME_SHINGLE, COLUMN_VALUE, COLUMN_FORMAT;

    private static double TABLE_NAME_JOIN_WEIGHT = 1;
    private static double COLUMN_NAME_JOIN_WEIGHT = 1;
    private static double COLUMN_VALUES_JOIN_WEIGHT = 1;
    private static double COLUMN_FORMAT_JOIN_WEIGHT = 1;

    private static double TABLE_NAME_UNION_WEIGHT = 1;
    private static double COLUMN_NAME_UNION_WEIGHT = 1;
    private static double COLUMN_VALUES_UNION_WEIGHT = 1;
    private static double COLUMN_FORMAT_UNION_WEIGHT = 1;

    public static double getJoinWeight(SimilarityMeasures measure) {
        return switch (measure) {
            case TABLE_NAME, TABLE_NAME_SHINGLE -> TABLE_NAME_JOIN_WEIGHT;
            case COLUMN_NAME, COLUMN_NAME_SHINGLE -> COLUMN_NAME_JOIN_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT;
        };
    }

    public static double getUnionWeight(SimilarityMeasures measure) {
        return switch (measure) {
            case TABLE_NAME, TABLE_NAME_SHINGLE -> TABLE_NAME_UNION_WEIGHT;
            case COLUMN_NAME, COLUMN_NAME_SHINGLE -> COLUMN_NAME_UNION_WEIGHT;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT;
        };
    }

    public static void setJoinWeight(SimilarityMeasures measure, double weight) {
        switch (measure) {
            case TABLE_NAME, TABLE_NAME_SHINGLE -> TABLE_NAME_JOIN_WEIGHT = weight;
            case COLUMN_NAME, COLUMN_NAME_SHINGLE -> COLUMN_NAME_JOIN_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_JOIN_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_JOIN_WEIGHT = weight;
        }
    }

    public static void setUnionWeight(SimilarityMeasures measure, double weight) {
        switch (measure) {
            case TABLE_NAME, TABLE_NAME_SHINGLE -> TABLE_NAME_UNION_WEIGHT = weight;
            case COLUMN_NAME, COLUMN_NAME_SHINGLE -> COLUMN_NAME_UNION_WEIGHT = weight;
            case COLUMN_VALUE -> COLUMN_VALUES_UNION_WEIGHT = weight;
            case COLUMN_FORMAT -> COLUMN_FORMAT_UNION_WEIGHT = weight;
        }
    }
}
