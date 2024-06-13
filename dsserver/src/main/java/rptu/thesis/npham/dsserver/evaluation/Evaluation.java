package rptu.thesis.npham.dsserver.evaluation;

public record Evaluation(int tp, int fp, int fn, double precision, double recall, int tp_table, int fp_table, int fn_table, double precision_table, double recall_table) {}
