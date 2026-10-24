"""
Build lake.gold.default_hazard: the monthly default and cure probabilities, per segment (step 4b).

Segment = credit band x loan-to-value band x loan age band (48 cells, defined in segments.py).

  default hazard = P(a loan moves INTO 90+ days late this month)
                 = events / months at risk
      at risk : the month before, the loan was under 90 days late (delinq_months < 3)
      event   : this month it is 90 or more days late (delinq_months >= 3)

  cure hazard    = P(a loan that was 90+ days late gets back under 90 days this month)
      at risk : the month before, the loan was 90+ days late
      event   : this month it is under 90 days late

Months where the status is RA (REO) or XX (unknown) have no delinq_months and are skipped.
A cell with fewer than --min-events events is flagged "thin"; nothing is smoothed in v1.

    spark-submit ... /opt/spark-jobs/build_default_hazard.py [--min-events 30]
"""
import argparse
import time

from pyspark.sql import Window, functions as F

from lake import build_spark
from segments import age_band, credit_band, ltv_band

TABLE = "lake.gold.default_hazard"
LATE_MONTHS = 3          # "90+ days late" = 3 or more months behind


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-events", type=int, default=30, help="cells with fewer default events are flagged thin")
    parser.add_argument("--before", default=None, help="back-test mode: only use performance rows before this date")
    parser.add_argument("--suffix", default="", help="back-test mode: appended to the output table name")
    args = parser.parse_args()
    started = time.time()
    table = TABLE + args.suffix

    spark = build_spark("build_default_hazard")
    loans = spark.table("lake.silver.loans").select("loan_seq", "credit_score", "ltv")
    performance = spark.table("lake.silver.loan_performance").select(
        "loan_seq", "report_period", "delinq_months", "loan_age")
    if args.before:
        performance = performance.filter(F.col("report_period") < F.lit(args.before).cast("date"))

    # ---- look at each month next to the month before it ----
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")
    previous = F.lag("delinq_months").over(by_loan)
    this_month_known = F.col("delinq_months").isNotNull()

    months = performance.withColumn("previous_delinq", previous)
    was_current = F.col("previous_delinq").isNotNull() & (F.col("previous_delinq") < LATE_MONTHS)
    was_late = F.col("previous_delinq") >= LATE_MONTHS

    marked = (months
              .withColumn("default_at_risk", was_current & this_month_known)
              .withColumn("default_event", was_current & this_month_known & (F.col("delinq_months") >= LATE_MONTHS))
              .withColumn("cure_at_risk", was_late & this_month_known)
              .withColumn("cure_event", was_late & this_month_known & (F.col("delinq_months") < LATE_MONTHS)))

    # ---- count per segment ----
    segmented = (marked.join(loans, "loan_seq")
                       .groupBy(credit_band(F.col("credit_score")).alias("credit_band"),
                                ltv_band(F.col("ltv")).alias("ltv_band"),
                                age_band(F.col("loan_age")).alias("age_band"))
                       .agg(F.sum(F.col("default_at_risk").cast("long")).alias("default_exposure_months"),
                            F.sum(F.col("default_event").cast("long")).alias("default_events"),
                            F.sum(F.col("cure_at_risk").cast("long")).alias("cure_exposure_months"),
                            F.sum(F.col("cure_event").cast("long")).alias("cures")))

    hazard = (segmented
              .withColumn("monthly_default_rate", F.col("default_events") / F.col("default_exposure_months"))
              .withColumn("monthly_cure_rate", F.col("cures") / F.col("cure_exposure_months"))
              .withColumn("thin", F.col("default_events") < args.min_events)
              .withColumn("calibrated_at", F.current_timestamp())
              .orderBy("credit_band", "ltv_band", "age_band"))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    hazard.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    # ---- report ----
    written = spark.table(table)
    cells = written.count()
    totals = written.agg(F.sum("default_events").alias("default_events"),
                         F.sum("default_exposure_months").alias("default_exposure"),
                         F.sum("cures").alias("cures"), F.sum("cure_exposure_months").alias("cure_exposure"),
                         F.sum(F.col("thin").cast("int")).alias("thin_cells")).collect()[0]
    print(f"\nwritten {table}: {cells} cells (expect 48) | {time.time() - started:.0f}s", flush=True)
    print(f"default events: {totals['default_events']:,} over {totals['default_exposure']:,} months at risk "
          f"= {totals['default_events'] / totals['default_exposure'] * 100:.4f}% per month", flush=True)
    print(f"cures: {totals['cures']:,} over {totals['cure_exposure']:,} months spent 90+ days late "
          f"= {totals['cures'] / totals['cure_exposure'] * 100:.2f}% per month", flush=True)
    print(f"thin cells (fewer than {args.min_events} default events): {totals['thin_cells']}", flush=True)
    print("\nall segments:", flush=True)
    (written.select("credit_band", "ltv_band", "age_band", "default_exposure_months", "default_events",
                    F.round(F.col("monthly_default_rate") * 100, 4).alias("default_pct_per_month"),
                    "cure_exposure_months", "cures",
                    F.round(F.col("monthly_cure_rate") * 100, 2).alias("cure_pct_per_month"), "thin")
            .orderBy("credit_band", "ltv_band", "age_band").show(60, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
