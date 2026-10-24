"""
Test run of the simulation engine on a SAMPLE of loans. Writes no tables.

Takes about 1,000 random alive loans, plus some loans that are 90+ days late and some in REO (so every starting
state is tested), runs the Spark engine, and applies three checks:

  1. Probabilities add up: current + late + prepaid + direct loss + REO + other exit = 1 for every loan
     (for loans that start in REO the walk is skipped, so all are 0).
  2. Zero-hazard test: if nothing can ever happen, the expected loss is 0 and the balance falls to 0 at maturity.
  3. Same answer as the plain-Python reference (simulation_reference.py) for every sampled loan.

    spark-submit ... /opt/spark-jobs/run_simulation.py [--sample-loans 1000] [--seed 7]
"""
import argparse
import time

from pyspark.sql import functions as F

from lake import build_spark
from segments import CREDIT_BANDS, LTV_BANDS
from sim_params import baseline_scenario, load_parameters
from simulation_reference import simulate_loan
from simulation_spark import add_simulation

PROBABILITY_TOLERANCE = 1e-9
BALANCE_TOLERANCE_DOLLARS = 0.01
REFERENCE_TOLERANCE = 1e-6          # dollars, or the same relative size


def report(label, passed, detail):
    print(f"{'PASS' if passed else 'FAIL'}  {label}  {detail}", flush=True)
    return passed


def take_sample(state, sample_loans, extra_late, extra_reo, seed):
    total = state.count()
    fraction = min(1.0, sample_loans * 1.3 / total)
    random_part = state.sample(False, fraction, seed).limit(sample_loans)
    late_part = state.filter((F.col("delinq_status") != "RA") & (F.col("delinq_months") >= 3)).limit(extra_late)
    reo_part = state.filter(F.col("delinq_status") == "RA").limit(extra_reo)
    return random_part.unionByName(late_part).unionByName(reo_part).dropDuplicates(["loan_seq"])


def run_timing(spark, params, scenario, loans_wanted, seed):
    """Time the engine on a larger sample and estimate the full book. No rows are collected and no checks run."""
    state = spark.table("lake.gold.loan_current_state")
    book = state.agg(F.count("*").alias("loans"), F.sum("remaining_months").alias("months")).collect()[0]
    sample = state.sample(False, min(1.0, loans_wanted * 1.05 / book["loans"]), seed).limit(loans_wanted).cache()
    sample_stats = sample.agg(F.count("*").alias("loans"), F.sum("remaining_months").alias("months")).collect()[0]   # loads the sample
    print(f"sample: {sample_stats['loans']:,} loans, {sample_stats['months']:,} loan-months "
          f"(whole book: {book['loans']:,} loans, {book['months']:,} loan-months)", flush=True)
    print(f"cores available to this job: {spark.sparkContext.defaultParallelism}", flush=True)

    for attempt in ("first pass (includes warm-up)", "second pass"):
        started = time.time()
        totals = add_simulation(sample, params, scenario).agg(
            F.count("*").alias("loans"), F.sum("current_upb").alias("balance"), F.sum("expected_loss").alias("loss")).collect()[0]
        seconds = time.time() - started
        print(f"{attempt}: {seconds:.1f}s | {seconds / sample_stats['months'] * 1e6:.2f} microseconds of wall time per loan-month "
              f"| expected loss ${totals['loss']:,.0f} = {totals['loss'] / float(totals['balance']) * 100:.4f}% of balance", flush=True)

    full_book_seconds = seconds * book["months"] / sample_stats["months"]
    print(f"\nIf the time grows in a straight line, the whole book on the same cores takes about "
          f"{full_book_seconds / 60:.0f} minutes per scenario ({full_book_seconds * 3 / 3600:.1f} hours for 3 scenarios). "
          f"An estimate, not a measurement of the full run.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timing", action="store_true", help="time the engine on --timing-loans loans instead of running the checks")
    parser.add_argument("--timing-loans", type=int, default=100000)
    parser.add_argument("--sample-loans", type=int, default=1000)
    parser.add_argument("--extra-late", type=int, default=50, help="extra loans that start 90+ days late")
    parser.add_argument("--extra-reo", type=int, default=20, help="extra loans that start in REO")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark("run_simulation_test")
    params = load_parameters(spark)
    scenario = baseline_scenario(params.market_rate)
    print(f"market rate {params.market_rate:.3f}% | REO sale probability {params.reo_sale_probability:.3f} | "
          f"LGD direct {params.lgd_direct:.4f} | LGD REO {params.lgd_reo:.4f}", flush=True)

    if args.timing:
        run_timing(spark, params, scenario, args.timing_loans, args.seed)
        spark.stop()
        return

    sample = take_sample(spark.table("lake.gold.loan_current_state"), args.sample_loans, args.extra_late, args.extra_reo, args.seed).cache()
    print(f"sample: {sample.count():,} loans", flush=True)

    started = time.time()
    rows = add_simulation(sample, params, scenario).collect()
    spark_seconds = time.time() - started
    print(f"Spark engine on {len(rows):,} loans: {spark_seconds:.1f}s (includes start-up)", flush=True)

    # ---- overview ----
    print("\nexpected loss by starting state (sample):", flush=True)
    for state in ("C", "L", "R"):
        group = [r for r in rows if r["starting_state"] == state]
        if group:
            balance = sum(float(r["current_upb"]) for r in group)
            loss = sum(r["expected_loss"] for r in group)
            print(f"   {state}: {len(group):5,} loans | balance ${balance:,.0f} | expected loss ${loss:,.0f} | {loss / balance * 100:.3f}% of balance", flush=True)

    print("\nchecks:", flush=True)
    all_passed = True

    # ---- 1. probabilities add up ----
    worst = 0.0
    for r in rows:
        total = r["p_current"] + r["p_late"] + r["prob_prepaid"] + r["prob_direct"] + r["prob_reo"] + r["prob_other"]
        worst = max(worst, abs(total - (0.0 if r["starting_state"] == "R" else 1.0)))
    all_passed &= report("probabilities add up to 1", worst < PROBABILITY_TOLERANCE, f"(largest error {worst:.2e})")

    # ---- 2. zero-hazard test ----
    quiet = {r["loan_seq"]: r for r in add_simulation(sample.filter(F.col("delinq_status") != "RA"),
                                                       params.with_zero_hazards(), scenario).collect()}
    biggest_loss = max(r["expected_loss"] for r in quiet.values())
    biggest_balance = max(r["balance_end"] for r in quiet.values())
    all_passed &= report("zero hazards give zero loss", biggest_loss == 0.0, f"(largest loss ${biggest_loss:.2f})")
    all_passed &= report("zero hazards: balance reaches 0 at maturity", biggest_balance < BALANCE_TOLERANCE_DOLLARS,
                         f"(largest balance left ${biggest_balance:.4f})")

    # ---- 3. same answer as the plain-Python reference ----
    worst_loss = worst_probability = 0.0
    for r in rows:
        reference = simulate_loan(
            dict(current_upb=r["current_upb"], current_rate=r["current_rate"], remaining_months=r["remaining_months"],
                 loan_age=r["loan_age"], credit_index=CREDIT_BANDS.index(r["credit_band"]),
                 ltv_index=LTV_BANDS.index(r["ltv_band"]), starting_state=r["starting_state"]), params, scenario)
        difference = abs(reference["expected_loss"] - r["expected_loss"])
        worst_loss = max(worst_loss, difference / max(1.0, abs(reference["expected_loss"])))
        for name in ("p_current", "p_late", "prob_prepaid", "prob_direct", "prob_reo", "prob_other"):
            worst_probability = max(worst_probability, abs(reference[name] - r[name]))
    all_passed &= report("Spark expected loss equals the Python reference", worst_loss < REFERENCE_TOLERANCE,
                         f"(largest relative difference {worst_loss:.2e})")
    all_passed &= report("Spark probabilities equal the Python reference", worst_probability < PROBABILITY_TOLERANCE,
                         f"(largest difference {worst_probability:.2e})")

    print(f"\nRESULT: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
