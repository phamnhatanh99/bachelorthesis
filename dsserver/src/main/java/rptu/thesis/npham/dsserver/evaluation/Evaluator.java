package rptu.thesis.npham.dsserver.evaluation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import rptu.thesis.npham.dscommon.utils.Constants;
import rptu.thesis.npham.dscommon.utils.StringUtils;
import rptu.thesis.npham.dscommon.model.query.QueryResults;
import rptu.thesis.npham.dsserver.model.ground_truth.GroundTruth;
import rptu.thesis.npham.dscommon.model.query.SingleResult;
import rptu.thesis.npham.dsserver.repository.GroundTruthRepo;
import rptu.thesis.npham.dscommon.utils.CSVReader;
import tech.tablesaw.api.Row;
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

    public void evaluate(QueryResults results) {
        System.out.println("Returned " + results.results().size() + " results");
        double[] eval = precisionAndRecall(results);
        System.out.println("Precision: " + eval[0]);
        System.out.println("Recall: " + eval[1]);
        System.out.println("F1: " + f1Score(eval[0], eval[1]));
    }

    public void clearGroundTruth() {
        ground_truth_repository.deleteAll();
    }

    public void loadGroundTruth(Datasets dataset) {
        String folder_path = "C:\\Users\\alexa\\Desktop\\Evaluation\\ground_truths";
        String file_name = Datasets.getCSVFile(dataset);
        String file_path = folder_path + "\\" + file_name;
        Path path = Paths.get(file_path);

        Table table = CSVReader.readTable(path, false);

        for (Row row : table) {
            String source_table = CSVReader.trimCSVSuffix(row.getString(0));
            String source_column = StringUtils.normalize(row.getString(1));
            String target_table = CSVReader.trimCSVSuffix(row.getString(2));
            String target_column = StringUtils.normalize(row.getString(3));
            if (source_table.equals(target_table) && source_column.equals(target_column)) continue;
            GroundTruth ground_truth = new GroundTruth(source_table, source_column, target_table, target_column);
            try {
                ground_truth_repository.save(ground_truth);
            } catch (DuplicateKeyException ignored) {}
        }
    }

    public double[] precisionAndRecall(QueryResults query_results) {
        double[] result = new double[2];

        int tp = 0;
        int fp = 0;
        int fn;

        Set<GroundTruth> ground_truths_set = new HashSet<>();

        for (SingleResult res : query_results.results()) {
            String query_table_name = res.query().getTableName();
            String query_column_name = res.query().getColumnName();
            String candidate_table_name = res.candidate().getTableName();
            String candidate_column_name = res.candidate().getColumnName();

            ground_truths_set.addAll(ground_truth_repository.findBySourceTableNameAndSourceColumnName(query_table_name, query_column_name));

            Optional<GroundTruth> ground_truth_o = ground_truth_repository.findBySourceTableNameAndTargetTableNameAndSourceColumnNameAndTargetColumnName(query_table_name, candidate_table_name, query_column_name, candidate_column_name);

            if (ground_truth_o.isPresent())
                tp++;
            else {
                fp++;
                System.out.println("False positive: " + query_column_name + " -> " + candidate_table_name + Constants.SEPARATOR + candidate_column_name);
            }
        }

        fn = ground_truths_set.size() - tp;

        System.out.println("Ground truth size: " + ground_truths_set.size());
        System.out.println("TP: " + tp);
        System.out.println("FP: " + fp);
        System.out.println("FN: " + fn);

        // Precision
        if (tp + fp == 0) result[0] = 1;
        else result[0] = (double) tp / (tp + fp);

        // Recall
        if (tp + fn == 0) result[1] = 1;
        else result[1] = (double) tp / (tp + fn);

        return result;
    }

    public double f1Score(double precision, double recall) {
        return 2 * precision * recall / (precision + recall);
    }
}
