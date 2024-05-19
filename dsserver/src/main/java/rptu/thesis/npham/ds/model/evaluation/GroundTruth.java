package rptu.thesis.npham.ds.model.evaluation;

public class GroundTruth {
    private final String folder_path = "C:\\Users\\alexa\\Desktop\\Evaluation\\ground_truths";
    private String file_name;

    public GroundTruth() {}

    public void loadGroundTruth(Datasets dataset) {
        file_name = Datasets.getCSVFile(dataset);
    }
}
