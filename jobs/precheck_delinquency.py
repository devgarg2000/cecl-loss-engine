"""
Pre-check for the hazard model. READ-ONLY: reads silver, writes nothing.

Event we study: a loan's month where it moves INTO 90+ days late
(delinq_months >= 3 this month, and < 3 the month before).

  1. How many loans ever reach 90+ days late, and how many later recover (cure)?
  2. How many events and how much exposure does each segment get?
     Segment = credit band x loan-to-value band x loan age band.
     Thin cells (few events) give noisy hazards, so we count them.

    spark-submit ... /opt/spark-jobs/precheck_delinquency.py
"""
from pyspark.sql import Window, functions as F

from lake import build_spark

MIN_EVENTS = 30      # a cell with fewer events than this is called "thin"


def banner(text):
    print("\n=== " + text + " ===", flush=True)


def main():
    spark = build_spark("precheck_delinquency")
    loans = spark.table("lake.silver.loans").select("loan_seq", "credit_score", "ltv")
    perf = spark.table("lake.silver.loan_performance").select(
        "loan_seq", "report_period", "delinq_months", "loan_age", "zero_bal_code")

    # ---- mark each month: was the loan at risk, and did the event happen ----
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")
    marked = (perf.withColumn("previous_delinq", F.lag("delinq_months").over(by_loan))
                  .withColumn("at_risk", F.col("previous_delinq").isNotNull() & (F.col("previous_delinq") < 3)
                                         & F.col("delinq_months").isNotNull())
                  .withColumn("event", F.col("at_risk") & (F.col("delinq_months") >= 3))
                  .cache())

    # ---- 1. loans and cures ----
    banner("1. loans reaching 90+ days late")
    first_event = (marked.filter("event").groupBy("loan_seq").agg(F.min("report_period").alias("first_90_month")))
    n_loans_ever = first_event.count()
    print(f"loans that ever reach 90+ days late: {n_loans_ever:,}", flush=True)

    after = (marked.join(first_event, "loan_seq")
                   .filter((F.col("report_period") > F.col("first_90_month")) & (F.col("delinq_months") < 3))
                   .select("loan_seq").distinct())
    n_cured = after.count()
    print(f"of those, loans that later have a month below 90 days late (cured at least once): {n_cured:,} "
          f"({n_cured / n_loans_ever:.1%})", flush=True)

    last_row = (marked.groupBy("loan_seq").agg(F.max(F.struct("report_period", "zero_bal_code")).alias("last"))
                      .select("loan_seq", F.col("last.zero_bal_code").alias("final_code")))
    print("\nhow the loans that reached 90+ days late ended (null = still alive):", flush=True)
    (first_event.join(last_row, "loan_seq").groupBy("final_code").count().orderBy(F.desc("count")).show())

    total_events = marked.filter("event").count()
    total_exposure = marked.filter("at_risk").count()
    print(f"events (moves into 90+): {total_events:,} | at-risk loan-months: {total_exposure:,} | "
          f"monthly rate: {total_events / total_exposure * 100:.4f}%", flush=True)

    # ---- 2. segments ----
    banner("2. segment sizes")
    credit_band = (F.when(F.col("credit_score").isNull(), "unknown").when(F.col("credit_score") < 620, "a: <620")
                    .when(F.col("credit_score") < 680, "b: 620-679").when(F.col("credit_score") < 740, "c: 680-739")
                    .otherwise("d: 740+"))
    ltv_band = (F.when(F.col("ltv").isNull(), "unknown").when(F.col("ltv") <= 60, "a: <=60")
                 .when(F.col("ltv") <= 80, "b: 61-80").when(F.col("ltv") <= 90, "c: 81-90").otherwise("d: >90"))
    age_band = (F.when(F.col("loan_age") < 12, "a: 0-11").when(F.col("loan_age") < 36, "b: 12-35").otherwise("c: 36+"))

    cells = (marked.filter("at_risk").join(loans, "loan_seq")
                   .groupBy(credit_band.alias("credit_band"), ltv_band.alias("ltv_band"), age_band.alias("age_band"))
                   .agg(F.count("*").alias("exposure_months"), F.sum(F.col("event").cast("int")).alias("events"))
                   .withColumn("monthly_rate_pct", F.round(F.col("events") / F.col("exposure_months") * 100, 4))
                   .cache())
    n_cells = cells.count()
    thin = cells.filter(F.col("events") < MIN_EVENTS).count()
    print(f"cells: {n_cells} | thin cells (fewer than {MIN_EVENTS} events): {thin}", flush=True)
    print("\nall cells, biggest exposure first:", flush=True)
    cells.orderBy(F.desc("exposure_months")).show(80, truncate=False)

    print("events by credit band alone (does the risk order make sense?):", flush=True)
    (cells.groupBy("credit_band").agg(F.sum("events").alias("events"), F.sum("exposure_months").alias("exposure"))
          .withColumn("monthly_rate_pct", F.round(F.col("events") / F.col("exposure") * 100, 4)).orderBy("credit_band").show())
    spark.stop()


if __name__ == "__main__":
    main()
