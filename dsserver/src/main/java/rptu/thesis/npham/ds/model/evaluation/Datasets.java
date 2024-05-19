package rptu.thesis.npham.ds.model.evaluation;

public enum Datasets {
    NEXTIAJD_XS, NEXTIAJD_S, TUS_S, TUS_L, CHEMBL22;

    public static String getCSVFile(Datasets dataset) {
        switch (dataset) {
            case NEXTIAJD_XS -> {
                return "nextiajd_testbedXS.csv";
            }
            default -> throw new RuntimeException("File not yet available");
        }
    }
}
