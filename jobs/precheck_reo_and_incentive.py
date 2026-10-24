"""
Two read-only checks before the state machine and the prepay hazard. Writes nothing.

Part 1 - REO. 13,879 loans had a loss event, but only 8,586 were 90+ days late the month before.
  Guess: many went through status RA (REO acquisition: the bank owns the property), which has no month count.
  We look at the status in the month BEFORE each loss event, at how long loans sit in RA, and how RA loans end.

Part 2 - Alive loans by rate gap (the loan's rate minus the market-rate proxy), at the as-of month.
  Also prints how many loans stand behind the proxy for each month around 2020 and for the latest months.

    spark-submit ... /opt/spark-jobs/precheck_reo_and_incentive.py [--market-months 3]
"""
import argparse

from pyspark.sql import Window, functions as F

from lake import build_spark
from segments import incentive_bucket

LOSS_CODES = ["02", "03", "09", "15"]
LATE_MONTHS = 3


def banner(text):
    print("\n=== " + text + " ===", flush=True)


def previous_state(previous_status, previous_months):
    return (F.when(previous_status.isNull(), "no previous month")
             .when(previous_status == "RA", "RA (REO)")
             .when(previous_status == "XX", "XX (unknown)")
             .when(previous_months >= LATE_MONTHS, "90+ days late")
             .otherwise("under 90 days late"))


def check_reo(spark):
    performance = spark.table("lake.silver.loan_performance").select(
        "loan_seq", "report_period", "delinq_status", "delinq_months", "zero_bal_code")
    by_loan = Window.partitionBy("loan_seq").orderBy("report_period")

    def with_previous(frame):
        return (frame.withColumn("previous_status", F.lag("delinq_status").over(by_loan))
                     .withColumn("previous_months", F.lag("delinq_months").over(by_loan))
                     .withColumn("previous_state", previous_state(F.col("previous_status"), F.col("previous_months"))))

    banner("1a. status in the month BEFORE each loss event")
    loss_loans = performance.filter(F.col("zero_bal_code").isin(LOSS_CODES)).select("loan_seq").distinct()
    loss_history = with_previous(performance.join(F.broadcast(loss_loans), "loan_seq"))
    loss_rows = loss_history.filter(F.col("zero_bal_code").isin(LOSS_CODES)).cache()
    print(f"loss-event rows: {loss_rows.count():,}", flush=True)
    loss_rows.groupBy("previous_state").count().orderBy(F.desc("count")).show(truncate=False)
    print("by zero balance code:", flush=True)
    (loss_rows.groupBy("zero_bal_code", "previous_state").count().orderBy("zero_bal_code", F.desc("count")).show(40, truncate=False))

    banner("1b. loans that went through REO (status RA)")
    ra_loans = performance.filter(F.col("delinq_status") == "RA").select("loan_seq").distinct()
    ra_history = with_previous(performance.join(F.broadcast(ra_loans), "loan_seq")).cache()
    print(f"loans with at least one RA month: {ra_loans.count():,}", flush=True)
    months_in_ra = ra_history.filter(F.col("delinq_status") == "RA").groupBy("loan_seq").count()
    months_in_ra.agg(F.round(F.avg("count"), 1).alias("avg_months_in_RA"),
                     F.percentile_approx("count", [0.5, 0.9]).alias("median_and_p90_months"),
                     F.max("count").alias("max_months")).show(truncate=False)
    print("status in the month before a loan FIRST shows RA:", flush=True)
    (ra_history.filter((F.col("delinq_status") == "RA") & (F.col("previous_status") != "RA"))
               .groupBy("previous_state").count().orderBy(F.desc("count")).show(truncate=False))
    print("how loans that reached RA end (final row's zero balance code; null = still in REO or otherwise open):", flush=True)
    (ra_history.groupBy("loan_seq").agg(F.max(F.struct("report_period", "zero_bal_code")).alias("last"))
               .select(F.col("last.zero_bal_code").alias("final_code")).groupBy("final_code").count()
               .orderBy(F.desc("count")).show())


def check_incentive(spark, market_months):
    loans = spark.table("lake.silver.loans")
    alive = spark.table("lake.gold.loan_current_state")

    monthly = (loans.groupBy("first_pay_date")
                    .agg(F.count("*").alias("loans"),
                         F.round(F.sum(F.col("int_rate") * F.col("orig_upb")) / F.sum("orig_upb"), 3).alias("weighted_rate"))
                    .orderBy("first_pay_date"))
    banner("2a. loans behind the market-rate proxy, by month of first payment")
    print("around 2020 (Oct 2019 to Mar 2021):", flush=True)
    monthly.filter((F.col("first_pay_date") >= "2019-10-01") & (F.col("first_pay_date") <= "2021-03-01")).show(30)
    print("latest months:", flush=True)
    latest_months = monthly.orderBy(F.desc("first_pay_date")).limit(8).orderBy("first_pay_date")
    latest_months.show()

    newest = monthly.agg(F.max("first_pay_date")).collect()[0][0]
    window_start = F.add_months(F.lit(newest), -(market_months - 1))
    market_rate = (loans.filter(F.col("first_pay_date") >= window_start)
                        .agg(F.sum(F.col("int_rate") * F.col("orig_upb")) / F.sum("orig_upb")).collect()[0][0])
    print(f"market-rate proxy for the as-of month = balance-weighted origination rate of loans whose first payment is in "
          f"the last {market_months} months up to {newest}: {float(market_rate):.3f}%", flush=True)

    banner("2b. alive loans by rate gap (loan rate minus proxy)")
    bucketed = alive.withColumn("incentive_bucket", incentive_bucket(F.col("current_rate") - F.lit(float(market_rate))))
    total_loans, total_balance = alive.count(), alive.agg(F.sum("current_upb")).collect()[0][0]
    (bucketed.groupBy("incentive_bucket")
             .agg(F.count("*").alias("loans"), F.round(F.sum("current_upb") / 1e9, 1).alias("balance_billion"))
             .withColumn("share_of_loans_pct", F.round(F.col("loans") / total_loans * 100, 1))
             .withColumn("share_of_balance_pct", F.round(F.col("balance_billion") * 1e9 / float(total_balance) * 100, 1))
             .orderBy("incentive_bucket").show(truncate=False))
    print("by rate gap and age band (loans):", flush=True)
    (bucketed.groupBy("incentive_bucket").pivot("age_band").count().orderBy("incentive_bucket").show(truncate=False))
    print("distribution of the current rate of alive loans (percent):", flush=True)
    alive.select(F.round(F.col("current_rate")).alias("rounded_rate")).groupBy("rounded_rate").count().orderBy("rounded_rate").show(20)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--market-months", type=int, default=3, help="how many of the newest first-payment months define today's market rate")
    args = parser.parse_args()
    spark = build_spark("precheck_reo_and_incentive")
    check_reo(spark)
    check_incentive(spark, args.market_months)
    spark.stop()


if __name__ == "__main__":
    main()
