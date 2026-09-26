"""
Pre-checks for step 4 (the simulation). READ-ONLY: reads silver, writes nothing.

Answers four questions with real numbers:
  1. How many loans are alive at the as-of month, and how big is the simulation?
  2. How many defaults happened in the history (are there enough to calibrate hazards)?
  3. Is there enough data to calibrate loss given default (LGD)?
  4. How do modified loans and stale loans (latest row older than the as-of month) look?

    spark-submit ... /opt/spark-jobs/precheck_simulation.py [--as-of 2026-03-01]
"""
import argparse

from pyspark.sql import functions as F

from lake import build_spark

LOSS_CODES = ["02", "03", "09", "15"]        # Freddie's loss events (decision D2)
SCENARIOS = 3


def banner(text):
    print("\n=== " + text + " ===", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default="2026-03-01", help="the month we simulate from (decision D1)")
    args = parser.parse_args()

    spark = build_spark("precheck_simulation")
    loans = spark.table("lake.silver.loans")
    perf = spark.table("lake.silver.loan_performance")

    # ---- 1. each loan's latest row, and whether the loan is alive ----
    banner(f"1. alive loans at {args.as_of}")
    latest = (perf.groupBy("loan_seq")
                  .agg(F.max(F.struct("report_period", "current_upb", "zero_bal_code", "remaining_months",
                                      "loan_age", "delinq_months", "modification_flag")).alias("last"))
                  .select("loan_seq", "last.*")
                  .cache())
    loans_with_history = latest.count()
    print(f"loans with performance rows: {loans_with_history:,}", flush=True)

    ended = F.col("zero_bal_code").isNotNull()
    alive = latest.filter(~ended & (F.col("current_upb") > 0))
    alive_count = alive.count()
    print(f"ended (has a zero balance code): {latest.filter(ended).count():,}", flush=True)
    print(f"alive (no code, balance > 0): {alive_count:,}", flush=True)
    print(f"no code but balance = 0 (odd): {latest.filter(~ended & (F.col('current_upb') <= 0)).count():,}", flush=True)

    print("\nlatest report month of alive loans (a loan not at the as-of month has gone quiet):", flush=True)
    alive.groupBy("report_period").count().orderBy(F.desc("report_period")).show(8)
    stale = alive.filter(F.col("report_period") < F.lit(args.as_of).cast("date")).count()
    print(f"alive loans whose latest row is BEFORE {args.as_of}: {stale:,}", flush=True)

    print("\nhow the ended loans ended (zero balance code):", flush=True)
    latest.filter(ended).groupBy("zero_bal_code").count().orderBy(F.desc("count")).show()

    sizes = alive.agg(F.sum("current_upb").alias("balance"), F.sum("remaining_months").alias("months"),
                      F.min("remaining_months").alias("min_rem"), F.max("remaining_months").alias("max_rem"),
                      F.sum((F.col("remaining_months").isNull()).cast("int")).alias("null_rem"),
                      F.sum((F.col("remaining_months") <= 0).cast("int")).alias("non_positive_rem")).collect()[0]
    print(f"total balance of alive loans: {sizes['balance']:,.0f}", flush=True)
    print(f"remaining months: min={sizes['min_rem']} max={sizes['max_rem']} null={sizes['null_rem']} "
          f"zero_or_negative={sizes['non_positive_rem']}", flush=True)
    cells_one = sizes["months"]
    print(f"simulation size = sum of remaining months = {cells_one:,} loan-months per scenario; "
          f"x {SCENARIOS} scenarios = {cells_one * SCENARIOS:,} cells", flush=True)

    # ---- 4. modified loans among the alive ----
    banner("4. modified loans among alive loans")
    alive.groupBy(F.coalesce("modification_flag", F.lit("(not modified)")).alias("modification_flag")).count().show()

    # ---- 2. defaults in the history ----
    banner("2. defaults in the history")
    total_months = perf.count()
    loss_event_rows = perf.filter(F.col("zero_bal_code").isin(LOSS_CODES)).count()
    late90_rows = perf.filter(F.col("delinq_months") >= 3).count()
    print(f"loan-months in history: {total_months:,}", flush=True)
    print(f"loss-event rows (codes {LOSS_CODES}): {loss_event_rows:,}", flush=True)
    print(f"rows 90+ days late (delinq_months >= 3): {late90_rows:,}", flush=True)
    print(f"loss events per 10,000 loan-months: {loss_event_rows / total_months * 10000:.2f}", flush=True)
    print("\nloss events per origination quarter (thin quarters give noisy hazards):", flush=True)
    (perf.filter(F.col("zero_bal_code").isin(LOSS_CODES)).groupBy("source_quarter").count()
         .orderBy("source_quarter").show(40))

    # ---- 3. LGD data ----
    banner("3. loss given default data")
    loss_rows = perf.filter(F.col("zero_bal_code").isin(LOSS_CODES))
    lgd = loss_rows.agg(F.count("*").alias("rows"),
                        F.sum(F.col("actual_loss").isNotNull().cast("int")).alias("with_actual_loss"),
                        F.sum(F.col("zero_bal_removal_upb").isNotNull().cast("int")).alias("with_removal_upb"),
                        F.sum("actual_loss").alias("sum_loss"),
                        F.sum("zero_bal_removal_upb").alias("sum_removed_upb")).collect()[0]
    print(f"loss-event rows: {lgd['rows']:,} | with actual_loss: {lgd['with_actual_loss']:,} | "
          f"with zero_bal_removal_upb: {lgd['with_removal_upb']:,}", flush=True)
    if lgd["sum_removed_upb"]:
        print(f"overall loss rate = sum(actual_loss) / sum(zero_bal_removal_upb) = "
              f"{lgd['sum_loss'] / lgd['sum_removed_upb']:.3f}", flush=True)
    print("\nactual_loss vs balance by zero balance code:", flush=True)
    (loss_rows.groupBy("zero_bal_code")
              .agg(F.count("*").alias("rows"), F.sum(F.col("actual_loss").isNotNull().cast("int")).alias("with_loss"),
                   F.round(F.sum("actual_loss") / F.sum("zero_bal_removal_upb"), 3).alias("loss_rate"))
              .orderBy("zero_bal_code").show())

    print(f"loans in silver.loans: {loans.count():,}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
