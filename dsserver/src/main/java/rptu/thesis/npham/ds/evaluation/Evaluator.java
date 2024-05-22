package rptu.thesis.npham.ds.evaluation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.ds.model.query.QueryResults;
import rptu.thesis.npham.ds.model.ground_truth.GroundTruth;
import rptu.thesis.npham.ds.repository.GroundTruthRepo;
import rptu.thesis.npham.ds.utils.CSVReader;
import rptu.thesis.npham.ds.utils.Pair;
import rptu.thesis.npham.ds.utils.StringUtils;
import tech.tablesaw.api.Table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Service
public class Evaluator {
    private final GroundTruthRepo ground_truth_repository;

    @Autowired
    public Evaluator(GroundTruthRepo ground_truth_repository) {
        this.ground_truth_repository = ground_truth_repository;
    }

    public void loadGroundTruth(Datasets dataset) {
        ground_truth_repository.deleteAll();

        String folder_path = "C:\\Users\\alexa\\Desktop\\Evaluation\\ground_truths";
        String file_name = Datasets.getCSVFile(dataset);
        String file_path = folder_path + "\\" + file_name;
        Path path = Paths.get(file_path);

        Table table = CSVReader.readTable(path, false);

        table.forEach(row ->
        {
            GroundTruth ground_truth = new GroundTruth(
                    CSVReader.trimCSVSuffix(row.getString(0)),
                    StringUtils.normalize(row.getString(1)),
                    CSVReader.trimCSVSuffix(row.getString(2)),
                    StringUtils.normalize(row.getString(3)));
            ground_truth_repository.save(ground_truth);
        });
    }

    public double[] precisionAndRecall(QueryResults query_results) {
        double[] result = new double[2];

        int tp = 0;
        int fp = 0;
        int fn;

        Set<GroundTruth> ground_truths_set = new HashSet<>();

        for (Pair<String, Pair<String, Double>> res : query_results.results()) {
            String[] query = res.first().split(StringUtils.SEPARATOR, 2);
            String query_table_name = query[0];
            String query_column_name = query[1];
            String[] candidate = res.second().first().split(StringUtils.SEPARATOR, 2);
            String candidate_table_name = candidate[0];
            String candidate_column_name = candidate[1];

            ground_truths_set.addAll(ground_truth_repository.findBySourceTableNameAndSourceColumnName(query_table_name, query_column_name));

            Optional<GroundTruth> ground_truth_o = ground_truth_repository.findBySourceTableNameAndTargetTableNameAndSourceColumnNameAndTargetColumnName(query_table_name, candidate_table_name, query_column_name, candidate_column_name);

            if (ground_truth_o.isPresent()) tp++;
            else fp++;
        }

        fn = ground_truths_set.size() - tp;

        if (tp + fp == 0) result[0] = 1;
        else result[0] = (double) tp / (tp + fp);

        if (tp + fn == 0) result[1] = 1;
        else result[1] = (double) tp / (tp + fn);

        return result;
    }

    public double f1Score(QueryResults query_results) {
        double[] pr = precisionAndRecall(query_results);
        return 2 * pr[0] * pr[1] / (pr[0] + pr[1]);
    }

    public double f1Score(double precision, double recall) {
        return 2 * precision * recall / (precision + recall);
    }
}
