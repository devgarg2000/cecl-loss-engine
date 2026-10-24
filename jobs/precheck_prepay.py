"""
Pre-check for the prepay hazard (step 4b4). READ-ONLY: reads silver, writes nothing.

A prepay event is a performance row with zero balance code 01 (voluntary payoff).
Monthly prepay rate = prepay events / loan-months observed.

Prepayment depends on the RATE INCENTIVE: the loan's current rate minus the prevailing market rate.
The data has no market-rate series, so we use a proxy:
    prevailing rate of a month = balance-weighted average origination rate of the loans whose
    first payment fell in that month.
Report months without any such loans (there are no 2020 origination quarters) get no incentive ("unknown").

Questions answered:
  1. Overall monthly prepay rate, and how much of it is really a payoff at maturity (not a prepayment)?
  2. Does the rate incentive matter?  (prepay rate per incentive bucket)
  3. How does prepay change by calendar year (the 2020-2021 refinance wave)?
  4. By segment (credit, LTV, age).
  5. How many report months have no prevailing rate?

    spark-submit ... /opt/spark-jobs/precheck_prepay.py
"""
from pyspark.sql import functions as F

from lake import build_spark
from segments import age_band, credit_band, incentive_bucket, ltv_band

VOLUNTARY_PAYOFF = "01"


def banner(text):
    print("\n=== " + text + " ===", flush=True)


def with_rate(frame, group_columns):
    """Sum events and months per group, then add the monthly rate in percent."""
    return (frame.groupBy(*group_columns)
                 .agg(F.sum("exposure_months").alias("exposure_months"), F.sum("prepay_events").alias("prepay_events"),
                      F.sum("maturity_payoffs").alias("maturity_payoffs"))
                 .withColumn("prepay_pct_per_month", F.round(F.col("prepay_events") / F.col("exposure_months") * 100, 4))
                 .orderBy(*group_columns))


def main():
    spark = build_spark("precheck_prepay")
    loans = spark.table("lake.silver.loans").select("loan_seq", "credit_score", "ltv", "first_pay_date", "int_rate", "orig_upb")
    performance = spark.table("lake.silver.loan_performance").select(
        "loan_seq", "report_period", "current_rate", "loan_age", "remaining_months", "zero_bal_code")

    # ---- proxy for the prevailing market rate, one value per month ----
    prevailing = (loans.groupBy(F.col("first_pay_date").alias("report_period"))
                       .agg((F.sum(F.col("int_rate") * F.col("orig_upb")) / F.sum("orig_upb")).alias("prevailing_rate")))
    print("prevailing rate proxy by year (average of the monthly values):", flush=True)
    (prevailing.groupBy(F.year("report_period").alias("year")).agg(F.round(F.avg("prevailing_rate"), 3).alias("avg_prevailing_rate"))
               .orderBy("year").show(20))

    # ---- one small table of counts: everything below is computed from it ----
    segment_of = loans.select("loan_seq", credit_band(F.col("credit_score")).alias("credit_band"),
                              ltv_band(F.col("ltv")).alias("ltv_band"))
    counts = (performance.join(segment_of, "loan_seq")
                         .join(F.broadcast(prevailing), "report_period", "left")
                         .groupBy("credit_band", "ltv_band",
                                  age_band(F.col("loan_age")).alias("age_band"),
                                  incentive_bucket(F.col("current_rate") - F.col("prevailing_rate")).alias("incentive_bucket"),
                                  F.year("report_period").alias("report_year"))
                         .agg(F.count(F.lit(1)).alias("exposure_months"),
                              F.sum((F.col("zero_bal_code") == VOLUNTARY_PAYOFF).cast("long")).alias("prepay_events"),
                              F.sum(((F.col("zero_bal_code") == VOLUNTARY_PAYOFF) & (F.col("remaining_months") <= 1)).cast("long"))
                               .alias("maturity_payoffs"))
                         .cache())
    print(f"count cells: {counts.count():,}", flush=True)

    banner("1. overall")
    total = counts.agg(F.sum("exposure_months").alias("m"), F.sum("prepay_events").alias("e"),
                       F.sum("maturity_payoffs").alias("mat")).collect()[0]
    print(f"loan-months: {total['m']:,} | prepay events (code 01): {total['e']:,} | monthly rate: {total['e'] / total['m'] * 100:.4f}%", flush=True)
    print(f"of those events, payoffs at maturity (remaining months 1 or less): {total['mat']:,} "
          f"({total['mat'] / total['e']:.2%}); these are not real prepayments", flush=True)

    banner("2. by rate incentive (does it matter?)")
    with_rate(counts, ["incentive_bucket"]).show(truncate=False)

    banner("3. by calendar year")
    with_rate(counts, ["report_year"]).show(20, truncate=False)

    banner("3b. share of loan-months with no prevailing rate ('unknown'), by year")
    (counts.groupBy("report_year")
           .agg((F.sum(F.when(F.col("incentive_bucket") == "unknown", F.col("exposure_months")).otherwise(0))
                 / F.sum("exposure_months") * 100).alias("unknown_pct"))
           .withColumn("unknown_pct", F.round("unknown_pct", 2)).orderBy("report_year").show(20))

    banner("4. by segment")
    with_rate(counts, ["credit_band"]).show(truncate=False)
    with_rate(counts, ["ltv_band"]).show(truncate=False)
    with_rate(counts, ["age_band"]).show(truncate=False)

    banner("5. rate incentive by year (prepay % per month; a wave should show up as high values in 2020-2021)")
    (with_rate(counts, ["report_year", "incentive_bucket"])
        .filter(F.col("exposure_months") >= 1_000_000).show(80, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
