"""
Steps 4b3 and 4c. Builds two gold tables.

1) lake.gold.late_transition  (what happens to a loan that is 90+ days late)
   For every loan-month where the PREVIOUS month was 90+ days late, exactly one outcome this month:
     loss_event    zero balance code 02, 03, 09 or 15
     other_exit    zero balance code 01, 16 or 96 (prepaid while late, reperforming sale, defect)
     unknown       no code, but the status is RA (REO) or XX (unknown), so no delinq_months
     cure          under 90 days late again
     still_late    still 90+ days late
   The outcomes do not overlap, so their rates add up to 1. Per segment (credit x LTV x age band).

2) lake.gold.lgd_by_ltv  (loss given default)
   loss rate = sum(actual_loss) / sum(zero_bal_removal_upb), counting ONLY rows that have an actual_loss
   (an earlier estimate divided by all balances and came out too low).
   Per zero balance code and LTV band, plus a blended row per LTV band with code 'all'.

    spark-submit ... /opt/spark-jobs/build_loss_and_lgd.py [--min-events 30]
"""
import argparse
import time

from pyspark.sql import Window, functions as F

from lake import build_spark
from segments import age_band, credit_band, ltv_band

LATE_TABLE = "lake.gold.late_transition"
LGD_TABLE = "lake.gold.lgd_by_ltv"
LATE_MONTHS = 3
LOSS_CODES = ["02", "03", "09", "15"]
OTHER_EXIT_CODES = ["01", "16", "96"]


def build_late_transition(spark, loans, performance, min_events, late_table):
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")
    months = (performance.select("loan_seq", "report_period", "delinq_months", "loan_age", "zero_bal_code")
                         .withColumn("previous_delinq", F.lag("delinq_months").over(by_loan))
                         .filter(F.col("previous_delinq") >= LATE_MONTHS))          # loans that were 90+ days late

    outcome = (F.when(F.col("zero_bal_code").isin(LOSS_CODES), "loss_event")
                .when(F.col("zero_bal_code").isin(OTHER_EXIT_CODES), "other_exit")
                .when(F.col("delinq_months").isNull(), "unknown")
                .when(F.col("delinq_months") < LATE_MONTHS, "cure")
                .otherwise("still_late"))

    segmented = (months.withColumn("outcome", outcome).join(loans, "loan_seq")
                       .groupBy(credit_band(F.col("credit_score")).alias("credit_band"),
                                ltv_band(F.col("ltv")).alias("ltv_band"),
                                age_band(F.col("loan_age")).alias("age_band"))
                       .agg(F.count(F.lit(1)).alias("late_months"),
                            F.sum((F.col("outcome") == "loss_event").cast("long")).alias("loss_events"),
                            F.sum((F.col("outcome") == "other_exit").cast("long")).alias("other_exits"),
                            F.sum((F.col("outcome") == "unknown").cast("long")).alias("unknown_status"),
                            F.sum((F.col("outcome") == "cure").cast("long")).alias("cures"),
                            F.sum((F.col("outcome") == "still_late").cast("long")).alias("still_late")))

    table = (segmented
             .withColumn("loss_rate", F.col("loss_events") / F.col("late_months"))
             .withColumn("cure_rate", F.col("cures") / F.col("late_months"))
             .withColumn("other_exit_rate", F.col("other_exits") / F.col("late_months"))
             .withColumn("unknown_rate", F.col("unknown_status") / F.col("late_months"))
             .withColumn("still_late_rate", F.col("still_late") / F.col("late_months"))
             .withColumn("thin", F.col("loss_events") < min_events)
             .withColumn("calibrated_at", F.current_timestamp()))
    table.writeTo(late_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    written = spark.table(late_table)
    totals = written.agg(F.count("*").alias("cells"), F.sum("late_months").alias("late_months"),
                         F.sum("loss_events").alias("loss_events"), F.sum("cures").alias("cures"),
                         F.sum("other_exits").alias("other_exits"), F.sum("unknown_status").alias("unknown_status"),
                         F.sum("still_late").alias("still_late"), F.sum(F.col("thin").cast("int")).alias("thin")).collect()[0]
    m = totals["late_months"]
    print(f"\n{late_table}: {totals['cells']} cells | months that started 90+ days late: {m:,}", flush=True)
    for name in ("loss_events", "cures", "other_exits", "unknown_status", "still_late"):
        print(f"   {name:15s} {totals[name]:>12,}  ({totals[name] / m:.3%})", flush=True)
    print(f"   thin cells (fewer than {min_events} loss events): {totals['thin']}", flush=True)
    print("\nby credit band:", flush=True)
    (written.groupBy("credit_band")
            .agg(F.sum("late_months").alias("late_months"), F.sum("loss_events").alias("loss_events"), F.sum("cures").alias("cures"))
            .withColumn("loss_pct", F.round(F.col("loss_events") / F.col("late_months") * 100, 3))
            .withColumn("cure_pct", F.round(F.col("cures") / F.col("late_months") * 100, 2)).orderBy("credit_band").show())
    print("all segments (loss and cure per 100 months that started 90+ days late):", flush=True)
    (written.select("credit_band", "ltv_band", "age_band", "late_months", "loss_events",
                    F.round(F.col("loss_rate") * 100, 3).alias("loss_pct"), F.round(F.col("cure_rate") * 100, 2).alias("cure_pct"), "thin")
            .orderBy("credit_band", "ltv_band", "age_band").show(60, truncate=False))


def build_lgd(spark, loans, performance, lgd_table):
    loss_rows = (performance.filter(F.col("zero_bal_code").isin(LOSS_CODES))
                            .select("loan_seq", "zero_bal_code", "actual_loss", "zero_bal_removal_upb")
                            .join(loans.select("loan_seq", "ltv"), "loan_seq")
                            .withColumn("ltv_band", ltv_band(F.col("ltv")))
                            .withColumn("has_loss", F.col("actual_loss").isNotNull()))

    def summarise(frame, code_column):
        return (frame.groupBy(code_column.alias("zero_bal_code"), "ltv_band")
                     .agg(F.count(F.lit(1)).alias("loss_event_rows"),
                          F.sum(F.col("has_loss").cast("long")).alias("rows_with_actual_loss"),
                          F.sum(F.when(F.col("has_loss"), F.col("actual_loss"))).alias("sum_actual_loss"),
                          F.sum(F.when(F.col("has_loss"), F.col("zero_bal_removal_upb"))).alias("sum_balance_of_rows_with_loss"),
                          F.sum(F.when(F.col("has_loss") & (F.col("actual_loss") < 0), 1).otherwise(0)).alias("rows_with_gain"),
                          F.percentile_approx(F.when(F.col("has_loss") & (F.col("zero_bal_removal_upb") > 0),
                                                     F.col("actual_loss") / F.col("zero_bal_removal_upb")), 0.5).alias("median_loss_rate")))

    by_code = summarise(loss_rows, F.col("zero_bal_code"))
    blended = summarise(loss_rows, F.lit("all"))
    table = (by_code.unionByName(blended)
             .withColumn("loss_rate", F.col("sum_actual_loss") / F.col("sum_balance_of_rows_with_loss"))
             .withColumn("calibrated_at", F.current_timestamp()))
    table.writeTo(lgd_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    written = spark.table(lgd_table)
    print(f"\n{lgd_table}: {written.count()} rows", flush=True)
    overall = (written.filter(F.col("zero_bal_code") == "all")
                      .agg(F.sum("loss_event_rows").alias("rows"), F.sum("rows_with_actual_loss").alias("with_loss"),
                           F.sum("sum_actual_loss").alias("loss"), F.sum("sum_balance_of_rows_with_loss").alias("balance"),
                           F.sum("rows_with_gain").alias("gains")).collect()[0])
    print(f"loss-event rows: {overall['rows']:,} | with actual_loss: {overall['with_loss']:,} | rows where the 'loss' is a gain: {overall['gains']:,}", flush=True)
    print(f"overall loss rate (only rows with an actual loss): {overall['loss'] / overall['balance']:.4f}", flush=True)
    print("\nby zero balance code and LTV band ('all' = blended across codes):", flush=True)
    (written.select("zero_bal_code", "ltv_band", "loss_event_rows", "rows_with_actual_loss", "rows_with_gain",
                    F.round("loss_rate", 4).alias("loss_rate"), F.round("median_loss_rate", 4).alias("median_loss_rate"))
            .orderBy("zero_bal_code", "ltv_band").show(40, truncate=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-events", type=int, default=30, help="cells with fewer loss events are flagged thin")
    parser.add_argument("--before", default=None, help="back-test mode: only use performance rows before this date")
    parser.add_argument("--suffix", default="", help="back-test mode: appended to both output table names")
    args = parser.parse_args()
    started = time.time()

    spark = build_spark("build_loss_and_lgd")
    loans = spark.table("lake.silver.loans").select("loan_seq", "credit_score", "ltv")
    performance = spark.table("lake.silver.loan_performance")
    if args.before:
        performance = performance.filter(F.col("report_period") < F.lit(args.before).cast("date"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

    build_late_transition(spark, loans, performance, args.min_events, LATE_TABLE + args.suffix)
    print(f"\nlate_transition done after {time.time() - started:.0f}s", flush=True)
    build_lgd(spark, loans, performance, LGD_TABLE + args.suffix)
    print(f"\nall done in {time.time() - started:.0f}s", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
