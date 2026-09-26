"""
Build lake.gold.loan_current_state: one row per ALIVE loan at the as-of month.
This is where the simulation starts.

A loan is alive when its latest performance row has no zero balance code and a balance above 0.
Loans with no performance rows at all are left out (decision D5); the job prints how many.

    spark-submit ... /opt/spark-jobs/build_loan_current_state.py [--as-of 2026-03-01]
"""
import argparse
import time

from pyspark.sql import functions as F

from lake import build_spark
from segments import age_band, credit_band, ltv_band

TABLE = "lake.gold.loan_current_state"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default="2026-03-01", help="the month we simulate from (decision D1)")
    args = parser.parse_args()
    started = time.time()

    spark = build_spark("build_loan_current_state")
    loans = spark.table("lake.silver.loans")
    performance = spark.table("lake.silver.loan_performance")

    # each loan's latest row: the struct is compared by its first field, report_period
    latest = (performance.groupBy("loan_seq")
                         .agg(F.max(F.struct("report_period", "current_upb", "loan_age", "remaining_months",
                                             "current_rate", "delinq_status", "delinq_months",
                                             "modification_flag", "zero_bal_code", "eltv")).alias("last"))
                         .select("loan_seq", "last.*"))

    alive = latest.filter(F.col("zero_bal_code").isNull() & (F.col("current_upb") > 0))
    stale = alive.filter(F.col("report_period") < F.lit(args.as_of).cast("date")).count()
    print(f"alive loans whose latest row is older than {args.as_of}: {stale:,} (they are still included, see check)", flush=True)

    state = (alive.join(loans, "loan_seq")
                  .select("loan_seq", "source_quarter",
                          F.col("report_period").alias("as_of_month"),
                          "current_upb", "loan_age", "remaining_months", "current_rate",
                          "delinq_status", "delinq_months", "modification_flag", "eltv",
                          "credit_score", "ltv", "cltv", "dti", "orig_upb", "orig_term",
                          F.col("int_rate").alias("orig_rate"),
                          "state", "occupancy", "purpose", "prop_type",
                          credit_band(F.col("credit_score")).alias("credit_band"),
                          ltv_band(F.col("ltv")).alias("ltv_band"),
                          age_band(F.col("loan_age")).alias("age_band"))
                  .sortWithinPartitions("loan_seq"))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    state.writeTo(TABLE).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    written = spark.table(TABLE)
    rows = written.count()
    distinct = written.select("loan_seq").distinct().count()
    print(f"\nwritten {TABLE}: {rows:,} rows | distinct loans: {distinct:,} | {time.time() - started:.0f}s", flush=True)
    written.agg(F.sum("current_upb").alias("total_balance"), F.sum("remaining_months").alias("remaining_months_total"),
                F.min("as_of_month").alias("first_month"), F.max("as_of_month").alias("last_month")).show(truncate=False)
    print("loans per segment band:", flush=True)
    written.groupBy("credit_band").count().orderBy("credit_band").show()
    written.groupBy("ltv_band").count().orderBy("ltv_band").show()
    written.groupBy("age_band").count().orderBy("age_band").show()
    spark.stop()


if __name__ == "__main__":
    main()
