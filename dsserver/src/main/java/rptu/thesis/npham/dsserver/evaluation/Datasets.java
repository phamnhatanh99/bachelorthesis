package rptu.thesis.npham.dsserver.evaluation;

public enum Datasets {
    NEXTIAJD_XS, TUS_S,
    MUSICIANS_JOINABLE, MUSICIANS_SEMJOINABLE, MUSICIANS_UNIONABLE, MUSICIANS_VIEWUNIONABLE,
    PROSPECT_JOINABLE, PROSPECT_SEMJOINABLE, PROSPECT_UNIONABLE, PROSPECT_VIEWUNIONABLE;

    public static String getCSVFile(Datasets dataset) {
        return switch (dataset) {
            case NEXTIAJD_XS -> "nextiajd_testbedXS.csv";
            case TUS_S -> "tus_s.csv";
            case MUSICIANS_JOINABLE -> "musicians_joinable.csv";
            case MUSICIANS_SEMJOINABLE -> "musicians_semjoinable.csv";
            case MUSICIANS_UNIONABLE -> "musicians_unionable.csv";
            case MUSICIANS_VIEWUNIONABLE -> "musicians_viewunionable.csv";
            case PROSPECT_JOINABLE -> "prospect_joinable.csv";
            case PROSPECT_SEMJOINABLE -> "prospect_semjoinable.csv";
            case PROSPECT_UNIONABLE -> "prospect_unionable.csv";
            case PROSPECT_VIEWUNIONABLE -> "prospect_viewunionable.csv";
        };
    }
}
