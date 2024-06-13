package rptu.thesis.npham.dsserver.evaluation;

public enum Datasets {
    NEXTIAJD_XS, TUS_S,
    MUSICIANS_JOINABLE, MUSICIANS_SEMJOINABLE, MUSICIANS_UNIONABLE, MUSICIANS_VIEWUNIONABLE,
    PROSPECT_JOINABLE_1, PROSPECT_JOINABLE_2, PROSPECT_JOINABLE_3,
    PROSPECT_SEMJOINABLE_1, PROSPECT_SEMJOINABLE_2, PROSPECT_SEMJOINABLE_3,
    PROSPECT_UNIONABLE_1, PROSPECT_UNIONABLE_2, PROSPECT_UNIONABLE_3,
    PROSPECT_VIEWUNIONABLE_1, PROSPECT_VIEWUNIONABLE_2, PROSPECT_VIEWUNIONABLE_3;

    public static Datasets fromString(String dataset_name) {
        return switch (dataset_name) {
            case "nextiajd_xs" -> NEXTIAJD_XS;
            case "tus_s" -> TUS_S;
            case "musicians_joinable" -> MUSICIANS_JOINABLE;
            case "musicians_semjoinable" -> MUSICIANS_SEMJOINABLE;
            case "musicians_unionable" -> MUSICIANS_UNIONABLE;
            case "musicians_viewunionable" -> MUSICIANS_VIEWUNIONABLE;
            case "prospect_joinable_1" -> PROSPECT_JOINABLE_1;
            case "prospect_joinable_2" -> PROSPECT_JOINABLE_2;
            case "prospect_joinable_3" -> PROSPECT_JOINABLE_3;
            case "prospect_semjoinable_1" -> PROSPECT_SEMJOINABLE_1;
            case "prospect_semjoinable_2" -> PROSPECT_SEMJOINABLE_2;
            case "prospect_semjoinable_3" -> PROSPECT_SEMJOINABLE_3;
            case "prospect_unionable_1" -> PROSPECT_UNIONABLE_1;
            case "prospect_unionable_2" -> PROSPECT_UNIONABLE_2;
            case "prospect_unionable_3" -> PROSPECT_UNIONABLE_3;
            case "prospect_viewunionable_1" -> PROSPECT_VIEWUNIONABLE_1;
            case "prospect_viewunionable_2" -> PROSPECT_VIEWUNIONABLE_2;
            case "prospect_viewunionable_3" -> PROSPECT_VIEWUNIONABLE_3;
            default -> throw new IllegalArgumentException("Invalid dataset name");
        };
    }

    public static String getGroundTruthFile(Datasets dataset) {
        String folder_path = "C:\\Users\\alexa\\Desktop\\Evaluation\\ground_truths\\";
        String file_name = switch (dataset) {
            case NEXTIAJD_XS -> "nextiajd_testbedXS.csv";
            case TUS_S -> "tus_s.csv";
            case MUSICIANS_JOINABLE -> "musicians_joinable.csv";
            case MUSICIANS_SEMJOINABLE -> "musicians_semjoinable.csv";
            case MUSICIANS_UNIONABLE -> "musicians_unionable.csv";
            case MUSICIANS_VIEWUNIONABLE -> "musicians_viewunionable.csv";
            case PROSPECT_JOINABLE_1 -> "prospect_joinable_1.csv";
            case PROSPECT_JOINABLE_2 -> "prospect_joinable_2.csv";
            case PROSPECT_JOINABLE_3 -> "prospect_joinable_3.csv";
            case PROSPECT_SEMJOINABLE_1 -> "prospect_semjoinable_1.csv";
            case PROSPECT_SEMJOINABLE_2 -> "prospect_semjoinable_2.csv";
            case PROSPECT_SEMJOINABLE_3 -> "prospect_semjoinable_3.csv";
            case PROSPECT_UNIONABLE_1 -> "prospect_unionable_1.csv";
            case PROSPECT_UNIONABLE_2 -> "prospect_unionable_2.csv";
            case PROSPECT_UNIONABLE_3 -> "prospect_unionable_3.csv";
            case PROSPECT_VIEWUNIONABLE_1 -> "prospect_viewunionable_1.csv";
            case PROSPECT_VIEWUNIONABLE_2 -> "prospect_viewunionable_2.csv";
            case PROSPECT_VIEWUNIONABLE_3 -> "prospect_viewunionable_3.csv";
        };
        return folder_path + file_name;
    }

    public static String getDatasetsFolder(Datasets dataset) {
        String base_path = "C:\\Users\\alexa\\Desktop\\Evaluation\\";
        String folder_path = switch (dataset) {
            case NEXTIAJD_XS -> "nextiajd\\testbedXS\\datasets";
            case TUS_S -> "table_union_search\\csv_small\\csv";
            case MUSICIANS_JOINABLE -> "valentine\\musicians_join\\csv";
            case MUSICIANS_SEMJOINABLE -> "valentine\\musicians_semjoin\\csv";
            case MUSICIANS_UNIONABLE -> "valentine\\musicians_union\\csv";
            case MUSICIANS_VIEWUNIONABLE -> "valentine\\musicians_viewunion\\csv";
            case PROSPECT_JOINABLE_1 -> "valentine\\prospect_join\\csv\\1";
            case PROSPECT_JOINABLE_2 -> "valentine\\prospect_join\\csv\\2";
            case PROSPECT_JOINABLE_3 -> "valentine\\prospect_join\\csv\\3";
            case PROSPECT_SEMJOINABLE_1 -> "valentine\\prospect_semjoin\\csv\\1";
            case PROSPECT_SEMJOINABLE_2 -> "valentine\\prospect_semjoin\\csv\\2";
            case PROSPECT_SEMJOINABLE_3 -> "valentine\\prospect_semjoin\\csv\\3";
            case PROSPECT_UNIONABLE_1 -> "valentine\\prospect_union\\csv\\1";
            case PROSPECT_UNIONABLE_2 -> "valentine\\prospect_union\\csv\\2";
            case PROSPECT_UNIONABLE_3 -> "valentine\\prospect_union\\csv\\3";
            case PROSPECT_VIEWUNIONABLE_1 -> "valentine\\prospect_viewunion\\csv\\1";
            case PROSPECT_VIEWUNIONABLE_2 -> "valentine\\prospect_viewunion\\csv\\2";
            case PROSPECT_VIEWUNIONABLE_3 -> "valentine\\prospect_viewunion\\csv\\3";
        };
        return base_path + folder_path;
    }
}
