package rptu.thesis.npham.dsserver.evaluation;

public enum Datasets {
    NEXTIAJD_TRAINING, NEXTIAJD_XS, NEXTIAJD_S, NEXTIAJD_M, TUS_S, TUS_L;

    public static String getCSVFile(Datasets dataset) {
        return switch (dataset) {
            case NEXTIAJD_TRAINING -> "nextiajd_training.csv";
            case NEXTIAJD_XS -> "nextiajd_testbedXS.csv";
            case NEXTIAJD_S -> "nextiajd_testbedS.csv";
            case NEXTIAJD_M -> "nextiajd_testbedM.csv";
            case TUS_S -> "tus_s.csv";
            case TUS_L -> "tus_l.csv";
        };
    }
}
