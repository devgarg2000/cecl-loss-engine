"""
What does delinquency status XX mean? READ-ONLY: reads silver and gold, writes nothing.

XX is not in Freddie's guide. It appears from report month 202510 on. 73,012 alive loans (1.2% of the balance) have it
at 2026-03, and the simulation engine needs to know how to start them.

  A. rows and loans with XX, per report month
  B. the status just before a loan first shows XX
  C. what happens the month after XX
  D. is it one servicer's habit?
  E. how alive XX loans differ from the other alive loans

Only report months from --from-month on are read (default 2025-06-01), which keeps the job small.

    spark-submit ... /opt/spark-jobs/precheck_xx_status.py [--from-month 2025-06-01]
"""
import argparse

from pyspark.sql import Window, functions as F

from lake import build_spark


def banner(text):
    print("\n=== " + text + " ===", flush=True)


def status_group(status, months):
    """A short label for a delinquency status."""
    return (F.when(status.isNull(), "(no previous month)")
             .when(status == "XX", "XX")
             .when(status == "RA", "RA (REO)")
             .when(months >= 3, "90+ days late")
             .when(months >= 1, "1-2 months late")
             .otherwise("current (0)"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-month", default="2025-06-01")
    args = parser.parse_args()

    spark = build_spark("precheck_xx_status")
    performance = (spark.table("lake.silver.loan_performance")
                        .select("loan_seq", "report_period", "delinq_status", "delinq_months", "zero_bal_code", "servicer_name")
                        .filter(F.col("report_period") >= F.lit(args.from_month).cast("date")))
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")
    marked = (performance.withColumn("previous_status", F.lag("delinq_status").over(by_loan))
                         .withColumn("previous_months", F.lag("delinq_months").over(by_loan))
                         .withColumn("next_status", F.lead("delinq_status").over(by_loan))
                         .withColumn("next_months", F.lead("delinq_months").over(by_loan))
                         .withColumn("next_code", F.lead("zero_bal_code").over(by_loan))
                         .cache())

    banner("A. XX rows per report month")
    (marked.groupBy("report_period")
           .agg(F.count("*").alias("rows"), F.sum((F.col("delinq_status") == "XX").cast("long")).alias("xx_rows"),
                F.countDistinct(F.when(F.col("delinq_status") == "XX", F.col("loan_seq"))).alias("xx_loans"))
           .withColumn("xx_share_pct", F.round(F.col("xx_rows") / F.col("rows") * 100, 3))
           .orderBy("report_period").show(20))

    banner("B. status just before a loan FIRST shows XX (the month before the run of XX starts)")
    starts = marked.filter((F.col("delinq_status") == "XX") & ((F.col("previous_status") != "XX") | F.col("previous_status").isNull()))
    (starts.withColumn("before", status_group(F.col("previous_status"), F.col("previous_months")))
           .groupBy("before").count().orderBy(F.desc("count")).show(truncate=False))

    banner("C. what happens the month AFTER an XX month")
    after = marked.filter(F.col("delinq_status") == "XX").filter(F.col("next_status").isNotNull() | F.col("next_code").isNotNull())
    (after.withColumn("after", F.when(F.col("next_code").isNotNull(), F.concat(F.lit("ends with code "), F.col("next_code")))
                               .otherwise(status_group(F.col("next_status"), F.col("next_months"))))
          .groupBy("after").count().orderBy(F.desc("count")).show(truncate=False))
    print("rows that are XX and the last row of the loan with no code (still open at the end of the data):", flush=True)
    print(f"   {marked.filter((F.col('delinq_status') == 'XX') & F.col('next_status').isNull() & F.col('next_code').isNull()).count():,}", flush=True)

    banner("D. servicers behind the XX rows (share of XX rows, and XX share of each servicer's own rows)")
    (marked.groupBy("servicer_name")
           .agg(F.count("*").alias("rows"), F.sum((F.col("delinq_status") == "XX").cast("long")).alias("xx_rows"))
           .withColumn("xx_share_of_servicer_pct", F.round(F.col("xx_rows") / F.col("rows") * 100, 2))
           .orderBy(F.desc("xx_rows")).show(12, truncate=False))

    banner("E. alive loans with XX at the as-of month compared with the other alive loans")
    alive = spark.table("lake.gold.loan_current_state")
    (alive.groupBy((F.col("delinq_status") == "XX").alias("is_xx"))
          .agg(F.count("*").alias("loans"), F.round(F.avg("credit_score"), 1).alias("avg_credit_score"),
               F.round(F.avg("ltv"), 1).alias("avg_ltv"), F.round(F.avg("loan_age"), 1).alias("avg_loan_age"),
               F.round(F.avg("current_rate"), 3).alias("avg_rate"), F.round(F.avg("current_upb"), 0).alias("avg_balance"),
               F.round(F.avg((F.col("modification_flag").isNotNull()).cast("int")) * 100, 2).alias("pct_modified"))
          .show(truncate=False))
    print("alive XX loans by age band:", flush=True)
    alive.filter(F.col("delinq_status") == "XX").groupBy("age_band").count().orderBy("age_band").show()
    spark.stop()


if __name__ == "__main__":
    main()
