"""
Build the REO tables (step 4b5). REO = Real Estate Owned: the bank owns the property after foreclosure
(delinquency status RA in the data). The REO sale is zero balance code 09.

1) lake.gold.reo_entry  - how often a 90+ days late loan enters REO, per age band x LTV band
     (credit is pooled because loss events are rare, decision E2).
     entry rate = loans entering REO / months that started 90+ days late
     The exposure (months that started 90+ days late) comes from lake.gold.late_transition,
     so build_loss_and_lgd.py must have run first.

2) lake.gold.reo_exit   - what happens in a month when the loan was already in REO the month before,
     by how many months it has already spent in REO (1 to 24; 24 means 24 or more):
       sold        zero balance code 09 (the REO sale)
       other_exit  any other zero balance code (01, 02, 03, 15, 16, 96)
       left_reo    no code, and the status is no longer RA
       still_reo   no code, still RA
   The months in REO are counted from the loan's own RA months, so a loan that returns to REO keeps counting.

Only loans that ever had an RA month are read (a few thousand loans), so this job is quick.

    spark-submit ... /opt/spark-jobs/build_reo_transition.py [--min-events 30]
"""
import argparse
import time

from pyspark.sql import Window, functions as F

from lake import build_spark
from segments import age_band, ltv_band

ENTRY_TABLE = "lake.gold.reo_entry"
EXIT_TABLE = "lake.gold.reo_exit"
LATE_MONTHS = 3
REO_SALE_CODE = "09"
MONTHS_CAP = 24


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-events", type=int, default=30, help="cells with fewer events are flagged thin")
    parser.add_argument("--before", default=None, help="back-test mode: only use performance rows before this date")
    parser.add_argument("--suffix", default="", help="back-test mode: appended to output tables and the late_transition table read")
    args = parser.parse_args()
    started = time.time()
    entry_table, exit_table = ENTRY_TABLE + args.suffix, EXIT_TABLE + args.suffix
    late_table = "lake.gold.late_transition" + args.suffix

    spark = build_spark("build_reo_transition")
    loans = spark.table("lake.silver.loans").select("loan_seq", "ltv")
    performance = spark.table("lake.silver.loan_performance").select(
        "loan_seq", "report_period", "delinq_status", "delinq_months", "loan_age", "zero_bal_code")
    if args.before:
        performance = performance.filter(F.col("report_period") < F.lit(args.before).cast("date"))

    # ---- the full history of every loan that was ever in REO ----
    reo_loans = performance.filter(F.col("delinq_status") == "RA").select("loan_seq").distinct()
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")
    up_to_now = by_loan.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    history = (performance.join(F.broadcast(reo_loans), "loan_seq")
                          .withColumn("previous_status", F.lag("delinq_status").over(by_loan))
                          .withColumn("previous_months", F.lag("delinq_months").over(by_loan))
                          .withColumn("reo_months_so_far", F.sum((F.col("delinq_status") == "RA").cast("int")).over(up_to_now))
                          .withColumn("reo_months_before", F.lag("reo_months_so_far").over(by_loan))
                          .cache())
    print(f"loans that were ever in REO: {reo_loans.count():,}", flush=True)

    # ---- 1) entering REO from the 90+ days late state ----
    entries = (history.filter((F.col("delinq_status") == "RA") & (F.col("previous_months") >= LATE_MONTHS))
                      .join(loans, "loan_seq")
                      .groupBy(age_band(F.col("loan_age")).alias("age_band"), ltv_band(F.col("ltv")).alias("ltv_band"))
                      .agg(F.count(F.lit(1)).alias("reo_entries")))
    exposure = (spark.table(late_table).groupBy("age_band", "ltv_band")
                     .agg(F.sum("late_months").alias("late_months")))
    entry_result = (exposure.join(entries, ["age_band", "ltv_band"], "left").fillna(0, ["reo_entries"])
                           .withColumn("monthly_reo_entry_rate", F.col("reo_entries") / F.col("late_months"))
                           .withColumn("thin", F.col("reo_entries") < args.min_events)
                           .withColumn("calibrated_at", F.current_timestamp()))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    entry_result.writeTo(entry_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    # ---- 2) what happens in a month that follows a REO month ----
    outcome = (F.when(F.col("zero_bal_code") == REO_SALE_CODE, "sold")
                .when(F.col("zero_bal_code").isNotNull(), "other_exit")
                .when(F.col("delinq_status") == "RA", "still_reo")
                .otherwise("left_reo"))
    exits = (history.filter(F.col("previous_status") == "RA")
                    .withColumn("outcome", outcome)
                    .groupBy(F.least(F.col("reo_months_before"), F.lit(MONTHS_CAP)).alias("months_in_reo"))
                    .agg(F.count(F.lit(1)).alias("exposure_months"),
                         F.sum((F.col("outcome") == "sold").cast("long")).alias("sold"),
                         F.sum((F.col("outcome") == "other_exit").cast("long")).alias("other_exit"),
                         F.sum((F.col("outcome") == "left_reo").cast("long")).alias("left_reo"),
                         F.sum((F.col("outcome") == "still_reo").cast("long")).alias("still_reo")))
    exit_result = (exits.withColumn("monthly_sale_rate", F.col("sold") / F.col("exposure_months"))
                       .withColumn("thin", F.col("exposure_months") < args.min_events)
                       .withColumn("calibrated_at", F.current_timestamp()))
    exit_result.writeTo(exit_table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    # ---- report ----
    entry = spark.table(entry_table)
    e = entry.agg(F.sum("late_months").alias("late"), F.sum("reo_entries").alias("entries"),
                  F.sum(F.col("thin").cast("int")).alias("thin")).collect()[0]
    print(f"\n{entry_table}: {entry.count()} cells | months that started 90+ late: {e['late']:,} | "
          f"REO entries: {e['entries']:,} ({e['entries'] / e['late'] * 100:.3f}% per month) | thin cells: {e['thin']}", flush=True)
    (entry.select("age_band", "ltv_band", "late_months", "reo_entries",
                  F.round(F.col("monthly_reo_entry_rate") * 100, 3).alias("entry_pct_per_month"), "thin")
          .orderBy("age_band", "ltv_band").show(20, truncate=False))

    leaving = spark.table(exit_table)
    t = leaving.agg(F.sum("exposure_months").alias("m"), F.sum("sold").alias("sold"), F.sum("other_exit").alias("other"),
                    F.sum("left_reo").alias("left"), F.sum("still_reo").alias("still")).collect()[0]
    print(f"{exit_table}: {leaving.count()} rows | months that followed a REO month: {t['m']:,} | sold {t['sold']:,} | "
          f"other exit {t['other']:,} | left REO {t['left']:,} | still in REO {t['still']:,}", flush=True)
    (leaving.select("months_in_reo", "exposure_months", "sold", "other_exit", "left_reo", "still_reo",
                    F.round(F.col("monthly_sale_rate") * 100, 2).alias("sale_pct_per_month"), "thin")
            .orderBy("months_in_reo").show(30, truncate=False))
    print(f"done in {time.time() - started:.0f}s", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
