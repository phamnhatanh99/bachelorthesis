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
import rptu.thesis.npham.dscommon.utils.CSV;
import tech.tablesaw.api.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class QueryEvaluator {
    private final GroundTruthRepo ground_truth_repository;

    @Autowired
    public QueryEvaluator(GroundTruthRepo ground_truth_repository) {
        this.ground_truth_repository = ground_truth_repository;
    }

    public void evaluate(QueryResults results, boolean is_join, int limit, double threshold, double elapsed) {
        System.out.println("Returned " + results.results().size() + " results");
        Evaluation eval = precisionAndRecall(results);
        System.out.println("Precision: " + eval.precision());
        System.out.println("Recall: " + eval.recall());

        if (results.results().isEmpty()) return;

        // Export to CSV
        String table_name = results.results().get(0).query().getTableName();
        int result_size = results.results().size();

        // TUS dataset
        if (limit == 401) {
            evaluate(results, is_join, 1, threshold, elapsed);
            evaluate(results, is_join, 5, threshold, elapsed);
            evaluate(results, is_join, 20, threshold, elapsed);
            evaluate(results, is_join, 50, threshold, elapsed);
            evaluate(results, is_join, 100, threshold, elapsed);
            evaluate(results, is_join, 200, threshold, elapsed);
            evaluate(results, is_join, 400, threshold, elapsed);
        }
        CSV.writeQueryResults(table_name, is_join, limit, threshold, result_size, elapsed,
                eval.tp(), eval.fp(), eval.fn(), eval.precision(), eval.recall(),
                eval.tp_table(), eval.fp_table(), eval.fn_table(), eval.precision_table(), eval.recall_table());
    }

    public void clearGroundTruth() {
        ground_truth_repository.deleteAll();
    }

    public void loadGroundTruth(Datasets dataset) {
        Path path = Paths.get(Datasets.getGroundTruthFile(dataset));

        Table table;
        try {
            table = CSV.readTable(path, false);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        for (Row row : table) {
            String source_table = CSV.trimCSVSuffix(row.getString(0));
            String source_column = StringUtils.normalize(row.getString(1));
            String target_table = CSV.trimCSVSuffix(row.getString(2));
            String target_column = StringUtils.normalize(row.getString(3));
            if (source_table.equals(target_table) && source_column.equals(target_column)) continue;
            GroundTruth ground_truth = new GroundTruth(source_table, source_column, target_table, target_column);
            try {
                ground_truth_repository.save(ground_truth);
            } catch (DuplicateKeyException ignored) {}
        }
    }

    /**
     * @return [precision, recall]
     */
    public Evaluation precisionAndRecall(QueryResults query_results) {
        int tp = 0;
        int fp = 0;
        int fn;

        Set<String> tp_tables = new HashSet<>();
        Set<String> fp_tables = new HashSet<>();
        Set<GroundTruth> ground_truths_set = new HashSet<>();

        for (SingleResult res : query_results.results()) {
            String query_table_name = res.query().getTableName();
            String query_column_name = res.query().getColumnName();
            String candidate_table_name = res.candidate().getTableName();
            String candidate_column_name = res.candidate().getColumnName();

            ground_truths_set.addAll(ground_truth_repository.findBySourceTableNameAndSourceColumnName(query_table_name, query_column_name));

            Optional<GroundTruth> ground_truth_o = ground_truth_repository.findBySourceTableNameAndTargetTableNameAndSourceColumnNameAndTargetColumnName(query_table_name, candidate_table_name, query_column_name, candidate_column_name);

            if (ground_truth_o.isPresent()) {
                tp++;
                tp_tables.add(candidate_table_name);
            }
            else {
                fp++;
                fp_tables.add(candidate_table_name);
                System.out.println("False positive: " + query_column_name + " -> " + candidate_table_name + Constants.SEPARATOR + candidate_column_name);
            }
        }

        Set<String> ground_truth_tables = ground_truths_set.stream().map(GroundTruth::getTargetTableName).collect(Collectors.toCollection(HashSet::new));

        int tp_tables_size = tp_tables.size();
        int fp_tables_size = fp_tables.size();
        int fn_tables_size = ground_truth_tables.size() - tp_tables_size;

        fn = ground_truths_set.size() - tp;

        System.out.println("Ground truth size: " + ground_truths_set.size());
        System.out.println("TP: " + tp);
        System.out.println("FP: " + fp);
        System.out.println("FN: " + fn);

        // Precision
        double precision;
        if (tp + fp == 0) precision = 1;
        else precision = (double) tp / (tp + fp);

        // Recall
        double recall;
        if (tp + fn == 0) recall = 1;
        else recall = (double) tp / (tp + fn);

        // Table precision
        double precision_table;
        if (tp_tables_size + fp_tables_size == 0) precision_table = 1;
        else precision_table = (double) tp_tables_size / (tp_tables_size + fp_tables_size);

        // Table recall
        double recall_table;
        if (tp_tables_size + fn_tables_size == 0) recall_table = 1;
        else recall_table = (double) tp_tables_size / (tp_tables_size + fn_tables_size);

        return new Evaluation(tp, fp, fn, precision, recall, tp_tables_size, fp_tables_size, fn_tables_size, precision_table, recall_table);
    }
}
