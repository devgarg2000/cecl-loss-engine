"""
Build lake.gold.sim_loan_result: the expected lifetime loss of every alive loan, saved one origination quarter at a time.
Resumable, like the bronze and silver builds.

For each quarter that is new or out of date:
  take that quarter's alive loans from lake.gold.loan_current_state
  -> run the simulation engine (simulation_spark.py)
  -> check the result (no empty or negative losses, probabilities add up)
  -> replace that quarter's slice of sim_loan_result (Iceberg partition overwrite, so a rerun never duplicates rows)
  -> check the saved row count
  -> write the ledger row into lake.gold.sim_manifest LAST.

The ledger row carries a FINGERPRINT of everything that affects the answer (the hazard numbers, the scenario, the as-of month,
the engine version) plus the size of the input. A quarter is skipped only when all of them still match, so changing a
hazard table automatically makes every saved quarter out of date.

    spark-submit ... /opt/spark-jobs/build_simulation.py [--scenario baseline] [--quarters 2023Q1 ..] [--years 2023 ..] [--dry-run] [--force]
"""
import argparse
import dataclasses
import hashlib
import json
import sys
import time

from pyspark.sql import functions as F

from lake import build_spark
from sim_params import adverse_scenario, baseline_scenario, load_parameters, severely_adverse_scenario
from simulation_spark import add_simulation

STATE_TABLE = "lake.gold.loan_current_state"
RESULT_TABLE = "lake.gold.sim_loan_result"
MANIFEST_TABLE = "lake.gold.sim_manifest"

ENGINE_VERSION = "e1"                # change it whenever the engine's rules change, so saved results become out of date
PROBABILITY_TOLERANCE = 1e-6
BALANCE_TOLERANCE_DOLLARS = 0.01
SCENARIOS = {"baseline": baseline_scenario, "adverse": adverse_scenario, "severely_adverse": severely_adverse_scenario}


def make_fingerprint(params, scenario, as_of_month):
    """A short label for everything that affects the result. Same fingerprint = same answer."""
    payload = {"engine": ENGINE_VERSION, "as_of_month": str(as_of_month),
               "parameters": dataclasses.asdict(params), "scenario": dataclasses.asdict(scenario)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
        loan_seq string COMMENT 'Loan id',
        scenario string COMMENT 'Scenario name, for example baseline',
        source_quarter string COMMENT 'Origination quarter of the loan',
        as_of_month date COMMENT 'The month the simulation starts from',
        starting_state string COMMENT 'C current, L 90+ days late, R in REO',
        credit_band string, ltv_band string,
        current_upb decimal(14,2) COMMENT 'Balance at the as-of month',
        expected_loss double COMMENT 'Expected lifetime credit loss in dollars',
        prob_prepaid double COMMENT 'Chance the loan is paid off early before maturity',
        prob_direct_loss double COMMENT 'Chance it ends in a loss through sale codes 02, 03, 15',
        prob_reo double COMMENT 'Chance it goes through REO (bank owns the property)',
        prob_other_exit double COMMENT 'Chance it leaves the late state without a loss (codes 01, 16, 96)',
        fingerprint string COMMENT 'Which hazards, scenario, as-of month and engine version produced this row',
        computed_at timestamp)
        USING iceberg PARTITIONED BY (scenario, source_quarter) TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
        scenario string, source_quarter string, fingerprint string, as_of_month date,
        input_loans bigint, input_balance double, loans_written bigint, loan_months bigint,
        expected_loss double, seconds double, status string, computed_at string) USING iceberg""")


def read_manifest(spark):
    if not spark.catalog.tableExists(MANIFEST_TABLE):
        return {}
    return {(r["scenario"], r["source_quarter"]): r.asDict() for r in spark.table(MANIFEST_TABLE).collect()}


def needs_work(existing, fingerprint, size, force):
    """Return the reason to (re)build this quarter, or None when the saved result is still valid."""
    if force:
        return "forced"
    if existing is None:
        return "new"
    if existing["status"] != "ok":
        return "previous attempt failed"
    if existing["fingerprint"] != fingerprint:
        return "hazards, scenario, as-of month or engine changed"
    if existing["input_loans"] != size["loans"] or abs(existing["input_balance"] - size["balance"]) > BALANCE_TOLERANCE_DOLLARS:
        return "the loans of this quarter changed"
    return None


def build_quarter(spark, quarter, scenario_name, params, scenario, fingerprint, size):
    """Simulate, check, save one quarter, and write its ledger row. Returns True when everything passed."""
    started = time.time()
    loans = spark.table(STATE_TABLE).filter(F.col("source_quarter") == quarter)
    checked = (add_simulation(loans, params, scenario)
               .select("loan_seq", F.lit(scenario_name).alias("scenario"), "source_quarter", "as_of_month", "starting_state",
                       "credit_band", "ltv_band", "current_upb", "expected_loss", "prob_prepaid",
                       F.col("prob_direct").alias("prob_direct_loss"), "prob_reo", F.col("prob_other").alias("prob_other_exit"),
                       F.lit(fingerprint).alias("fingerprint"), F.current_timestamp().alias("computed_at"),
                       "p_current", "p_late")
               .cache())

    # ---- check the result BEFORE saving it ----
    probability_error = F.abs(F.col("p_current") + F.col("p_late") + F.col("prob_prepaid") + F.col("prob_direct_loss")
                              + F.col("prob_reo") + F.col("prob_other_exit")
                              - F.when(F.col("starting_state") == "R", 0.0).otherwise(1.0))
    stats = checked.agg(F.count("*").alias("loans"), F.sum(F.col("expected_loss").isNull().cast("int")).alias("empty_losses"),
                        F.sum((F.col("expected_loss") < 0).cast("int")).alias("negative_losses"),
                        F.max(probability_error).alias("worst_probability_error"),
                        F.sum("expected_loss").alias("expected_loss")).collect()[0]
    problems = []
    if stats["loans"] != size["loans"]:
        problems.append(f"{stats['loans']:,} results for {size['loans']:,} loans")
    if stats["empty_losses"] or stats["negative_losses"]:
        problems.append(f"{stats['empty_losses']} empty and {stats['negative_losses']} negative losses")
    if stats["worst_probability_error"] is None or stats["worst_probability_error"] > PROBABILITY_TOLERANCE:
        problems.append(f"probabilities off by {stats['worst_probability_error']}")

    written = 0
    if not problems:
        checked.select(spark.table(RESULT_TABLE).columns).writeTo(RESULT_TABLE).overwritePartitions()
        written = (spark.table(RESULT_TABLE)
                        .filter((F.col("scenario") == scenario_name) & (F.col("source_quarter") == quarter)).count())
        if written != size["loans"]:
            problems.append(f"saved {written:,} rows for {size['loans']:,} loans")
    checked.unpersist()

    status = "failed" if problems else "ok"
    seconds = round(time.time() - started, 1)
    spark.sql(f"DELETE FROM {MANIFEST_TABLE} WHERE scenario = '{scenario_name}' AND source_quarter = '{quarter}'")
    # Built with Spark SQL expressions (F.lit, on an empty base row), NOT spark.createDataFrame(python_list, ...).
    # The latter ships the row through a Python worker process on the executor to deserialize it — fine when
    # driver and executor are the same Python (every job run by hand), but fails with PYTHON_VERSION_MISMATCH
    # when they differ (confirmed: Airflow's driver runs Python 3.11, the Spark workers run Python 3.12).
    (spark.range(1).select(
        F.lit(scenario_name).alias("scenario"), F.lit(quarter).alias("source_quarter"),
        F.lit(fingerprint).alias("fingerprint"), F.lit(str(size["as_of_month"])).cast("date").alias("as_of_month"),
        F.lit(size["loans"]).cast("bigint").alias("input_loans"), F.lit(size["balance"]).cast("double").alias("input_balance"),
        F.lit(written).cast("bigint").alias("loans_written"), F.lit(size["loan_months"]).cast("bigint").alias("loan_months"),
        F.lit(float(stats["expected_loss"] or 0.0)).alias("expected_loss"), F.lit(seconds).alias("seconds"),
        F.lit(status).alias("status"), F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss").alias("computed_at"))
     .writeTo(MANIFEST_TABLE).append())                          # the ledger row is written LAST

    balance = size["balance"]
    print(f"[{scenario_name}] {quarter}: {status}  loans={size['loans']:,} loan-months={size['loan_months']:,}  "
          f"expected loss=${(stats['expected_loss'] or 0):,.0f} ({(stats['expected_loss'] or 0) / balance * 100:.4f}% of balance)  "
          f"{seconds}s ({seconds / size['loan_months'] * 1e6:.2f} microseconds per loan-month)"
          + (f"  PROBLEMS: {'; '.join(problems)}" if problems else ""), flush=True)
    return not problems


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="baseline", choices=list(SCENARIOS))
    parser.add_argument("--quarters", nargs="*", help="only these origination quarters, e.g. 2023Q1")
    parser.add_argument("--years", nargs="*", help="only these origination years, e.g. 2023")
    parser.add_argument("--dry-run", action="store_true", help="show the plan and change nothing")
    parser.add_argument("--force", action="store_true", help="rebuild the selected quarters even if they are up to date")
    args = parser.parse_args()

    spark = build_spark("build_simulation")
    params = load_parameters(spark)
    scenario = SCENARIOS[args.scenario](params.market_rate)

    state = spark.table(STATE_TABLE)
    as_of_months = [r[0] for r in state.select("as_of_month").distinct().collect()]
    if len(as_of_months) != 1:
        raise ValueError(f"loan_current_state should have exactly one as-of month, found {as_of_months}")
    as_of_month = as_of_months[0]
    fingerprint = make_fingerprint(params, scenario, as_of_month)
    print(f"scenario {args.scenario} | as-of {as_of_month} | fingerprint {fingerprint} | "
          f"market rate {params.market_rate:.3f}% | cores {spark.sparkContext.defaultParallelism}", flush=True)

    sizes = {r["source_quarter"]: dict(loans=r["loans"], balance=float(r["balance"]), loan_months=r["loan_months"], as_of_month=as_of_month)
             for r in state.groupBy("source_quarter").agg(F.count("*").alias("loans"), F.sum("current_upb").alias("balance"),
                                                          F.sum("remaining_months").alias("loan_months")).collect()}
    quarters = sorted(sizes)
    if args.quarters:
        quarters = [q for q in quarters if q in args.quarters]
    if args.years:
        quarters = [q for q in quarters if q[:4] in args.years]

    manifest = read_manifest(spark)
    plan = []
    for quarter in quarters:
        reason = needs_work(manifest.get((args.scenario, quarter)), fingerprint, sizes[quarter], args.force)
        if reason:
            plan.append((quarter, reason))
    print(f"Quarters considered: {len(quarters)} | up to date: {len(quarters) - len(plan)} | to do: {len(plan)}", flush=True)
    for quarter, reason in plan:
        print(f"  will build {quarter}  ({reason})  loans={sizes[quarter]['loans']:,}  loan-months={sizes[quarter]['loan_months']:,}", flush=True)

    if args.dry_run or not plan:
        print("Dry run, nothing built." if args.dry_run else "Nothing to do.")
        spark.stop()
        return

    ensure_tables(spark)
    started, failures = time.time(), 0
    for quarter, _reason in plan:
        try:
            if not build_quarter(spark, quarter, args.scenario, params, scenario, fingerprint, sizes[quarter]):
                failures += 1
        except Exception as error:                     # keep going: one bad quarter must not stop the others
            failures += 1
            print(f"[{args.scenario}] {quarter}: ERROR {type(error).__name__}: {str(error)[:400]}", flush=True)
    print(f"Finished {len(plan)} quarter(s) in {time.time() - started:.0f}s, {failures} failure(s)", flush=True)
    spark.stop()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
