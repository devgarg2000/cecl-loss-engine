"""
Build lake.gold.prepay_hazard: the monthly prepay probability by rate gap and loan age (step 4b4).

  prepay hazard = P(a loan pays off early this month) = prepay events / loan-months observed
    event   : a performance row with zero balance code 01 (voluntary payoff)
    segment : rate-gap bucket x age band (5 x 3 = 15 cells)
    rate gap: the loan's current rate minus the market-rate proxy of that report month
    proxy   : balance-weighted origination rate of the loans whose first payment fell in that month

Only report months from --from-month on are used (decision E4: 2022 onward). The 2020-2021 refinance wave would
overstate prepayment for today's book, and the proxy is thin for 2020. The job prints how many loans stand
behind the proxy in the months it uses, so a thin proxy is visible.

    spark-submit ... /opt/spark-jobs/build_prepay_hazard.py [--from-month 2022-01-01] [--min-events 30]
"""
import argparse
import time

from pyspark.sql import functions as F

from lake import build_spark
from segments import age_band, incentive_bucket

TABLE = "lake.gold.prepay_hazard"
VOLUNTARY_PAYOFF = "01"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-month", default="2022-01-01", help="first report month used for calibration (decision E4)")
    parser.add_argument("--min-events", type=int, default=30, help="cells with fewer prepay events are flagged thin")
    parser.add_argument("--before", default=None, help="back-test mode: only use performance rows before this date")
    parser.add_argument("--suffix", default="", help="back-test mode: appended to the output table name")
    args = parser.parse_args()
    started = time.time()
    table = TABLE + args.suffix

    spark = build_spark("build_prepay_hazard")
    loans = spark.table("lake.silver.loans").select("loan_seq", "first_pay_date", "int_rate", "orig_upb")
    performance = (spark.table("lake.silver.loan_performance")
                        .select("loan_seq", "report_period", "current_rate", "loan_age", "zero_bal_code")
                        .filter(F.col("report_period") >= F.lit(args.from_month).cast("date")))
    if args.before:
        performance = performance.filter(F.col("report_period") < F.lit(args.before).cast("date"))

    market_rate = (loans.groupBy(F.col("first_pay_date").alias("report_period"))
                        .agg((F.sum(F.col("int_rate") * F.col("orig_upb")) / F.sum("orig_upb")).alias("market_rate"),
                             F.count("*").alias("loans_behind_rate")))
    thinnest = (market_rate.filter(F.col("report_period") >= F.lit(args.from_month).cast("date"))
                           .agg(F.min("loans_behind_rate").alias("fewest"), F.max("loans_behind_rate").alias("most")).collect()[0])
    print(f"loans behind the market-rate proxy, per report month from {args.from_month}: "
          f"fewest {thinnest['fewest']:,}, most {thinnest['most']:,}", flush=True)

    counts = (performance.join(F.broadcast(market_rate), "report_period", "left")
                         .groupBy(incentive_bucket(F.col("current_rate") - F.col("market_rate")).alias("incentive_bucket"),
                                  age_band(F.col("loan_age")).alias("age_band"))
                         .agg(F.count(F.lit(1)).alias("exposure_months"),
                              F.sum((F.col("zero_bal_code") == VOLUNTARY_PAYOFF).cast("long")).alias("prepay_events"),
                              F.max("report_period").alias("calibrated_to")))

    hazard = (counts.withColumn("monthly_prepay_rate", F.col("prepay_events") / F.col("exposure_months"))
                    .withColumn("thin", F.col("prepay_events") < args.min_events)
                    .withColumn("calibrated_from", F.lit(args.from_month).cast("date"))
                    .withColumn("calibrated_at", F.current_timestamp()))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    hazard.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    written = spark.table(table)
    totals = written.agg(F.count("*").alias("cells"), F.sum("exposure_months").alias("months"), F.sum("prepay_events").alias("events"),
                         F.sum(F.col("thin").cast("int")).alias("thin_cells")).collect()[0]
    print(f"\nwritten {table}: {totals['cells']} cells | {time.time() - started:.0f}s", flush=True)
    print(f"loan-months: {totals['months']:,} | prepay events: {totals['events']:,} | "
          f"monthly rate: {totals['events'] / totals['months'] * 100:.4f}% | thin cells: {totals['thin_cells']}", flush=True)
    (written.select("incentive_bucket", "age_band", "exposure_months", "prepay_events",
                    F.round(F.col("monthly_prepay_rate") * 100, 4).alias("prepay_pct_per_month"), "thin")
            .orderBy("incentive_bucket", "age_band").show(30, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
