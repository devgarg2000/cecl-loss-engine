"""
Back-test (step 4e, check 5): the strongest test of the simulation.

Everything before --cutoff is used to calibrate hazards and find which loans were alive then
(lake.gold.*_bt tables, built by run_backtest.sh before this script runs). This script:
  1. Runs the engine on the loans alive at --cutoff, using ONLY pre-cutoff hazards.
  2. Looks up what those same loans actually did between --cutoff and now (real history).
  3. Compares predicted vs actual, in dollars and in event counts.

Nothing here touches the live tables (loan_current_state, default_hazard, ...) used for the real run.

    spark-submit ... /opt/spark-jobs/run_backtest.py --cutoff 2023-01-01
"""
import argparse

from pyspark.sql import functions as F

from lake import build_spark
from sim_params import baseline_scenario, load_parameters
from simulation_spark import add_simulation

LOSS_CODES = ["02", "03", "09", "15"]
PREPAY_CODE = "01"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", required=True, help="e.g. 2023-01-01: calibrate on data before this, compare against what really happened after")
    args = parser.parse_args()

    spark = build_spark("run_backtest")
    params = load_parameters(spark, suffix="_bt", as_of=args.cutoff)
    scenario = baseline_scenario(params.market_rate)
    print(f"back-test cutoff {args.cutoff} | market rate then {params.market_rate:.3f}%", flush=True)

    # ---- step 1: run the engine on the loans alive at cutoff ----
    backtest_loans = spark.table("lake.gold.loan_current_state_bt")
    n_loans = backtest_loans.count()
    print(f"loans alive at cutoff: {n_loans:,}", flush=True)

    results = add_simulation(backtest_loans, params, scenario).select(
        "loan_seq", "current_upb", "expected_loss", "prob_prepaid", "prob_direct", "prob_reo").cache()
    results.writeTo("lake.gold.sim_loan_result_bt").using("iceberg").tableProperty("format-version", "2").createOrReplace()
    predicted = results.agg(F.sum("current_upb").alias("balance"), F.sum("expected_loss").alias("loss"),
                            F.sum("prob_prepaid").alias("prepaid"), F.sum("prob_direct").alias("direct"),
                            F.sum("prob_reo").alias("reo")).collect()[0]

    # ---- step 2: what actually happened to those loans, from cutoff to now ----
    loan_ids = backtest_loans.select("loan_seq")
    after = (spark.table("lake.silver.loan_performance")
                  .filter(F.col("report_period") >= F.lit(args.cutoff).cast("date"))
                  .join(loan_ids, "loan_seq"))
    actual_prepaid = after.filter(F.col("zero_bal_code") == PREPAY_CODE).select("loan_seq").distinct().count()
    actual_loss_rows = after.filter(F.col("zero_bal_code").isin(LOSS_CODES))
    actual_loss_loans = actual_loss_rows.select("loan_seq").distinct().count()
    actual_loss_dollars = actual_loss_rows.agg(F.sum("actual_loss")).collect()[0][0] or 0.0
    still_open = (after.groupBy("loan_seq").agg(F.max("zero_bal_code").alias("last_code"))
                       .filter(F.col("last_code").isNull()).count())

    # ---- step 3: compare ----
    print("\n==================== PREDICTED (at cutoff, for the whole remaining life) ====================", flush=True)
    print(f"expected lifetime loss: ${predicted['loss']:,.0f}  ({predicted['loss'] / float(predicted['balance']) * 100:.4f}% of balance)", flush=True)
    print(f"expected prepay count (probability-weighted): {predicted['prepaid']:,.0f}", flush=True)
    print(f"expected default-loss count (probability-weighted): {predicted['direct'] + predicted['reo']:,.0f}", flush=True)

    print(f"\n==================== ACTUAL (from {args.cutoff} to now, real history) ====================", flush=True)
    print(f"loans that prepaid: {actual_prepaid:,} of {n_loans:,} ({actual_prepaid / n_loans:.2%})", flush=True)
    print(f"loans with a loss event: {actual_loss_loans:,} of {n_loans:,} ({actual_loss_loans / n_loans:.2%})", flush=True)
    print(f"actual realized loss so far: ${actual_loss_dollars:,.0f}", flush=True)
    print(f"loans still current/late, not yet resolved (so losses on these may still be coming): {still_open:,}", flush=True)

    print("\nNote: this compares a lifetime prediction against only ~3 years of real outcomes, and some loans "
          "above are still unresolved, so actual losses will keep rising. Treat 'actual' as a lower bound.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
