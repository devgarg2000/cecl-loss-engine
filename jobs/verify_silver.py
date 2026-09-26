"""
Proof checks for the SILVER tables. READ-ONLY. Prints PASS / FAIL per check.

    spark-submit ... /opt/spark-jobs/verify_silver.py [--quarters 2023Q1 ...]

Without --quarters it checks every quarter that is built.
"""
import argparse
import sys

from pyspark.sql import functions as F

from lake import build_spark
from silver_schema import LOANS, PERF

fails = []


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}  {detail}", flush=True)
    if not ok:
        fails.append(label)


def banner(t):
    print("\n=== " + t + " ===", flush=True)


def leftover_placeholders(df, spec):
    """Placeholder values that should have become null must no longer exist."""
    total, parts = 0, []
    for c in spec:
        if not c.null_if or c.kind == "dec":
            continue
        vals = [int(v) if c.kind == "int" else v for v in c.null_if]
        n = df.filter(F.col(c.name).isin(*vals)).count()
        total += n
        if n:
            parts.append(f"{c.name}={n}")
    return total, ", ".join(parts)


def minmax(df, spec, kinds):
    cols = [c.name for c in spec if c.kind in kinds]
    r = df.agg(*[F.min(c).alias(f"{c}__min") for c in cols], *[F.max(c).alias(f"{c}__max") for c in cols]).collect()[0]
    return {c: (r[f"{c}__min"], r[f"{c}__max"]) for c in cols}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarters", nargs="*")
    args = ap.parse_args()
    spark = build_spark("verify_silver")

    def load(t):
        df = spark.table(t)
        return df.filter(F.col("source_quarter").isin(*args.quarters)) if args.quarters else df

    loans, perf = load("lake.silver.loans"), load("lake.silver.loan_performance")
    ref = spark.table("lake.silver.ref_zero_balance_codes")
    scope = ",".join(args.quarters) if args.quarters else "all built quarters"
    print(f"Scope: {scope}", flush=True)

    banner("column types")
    for name, df in (("loans", loans), ("loan_performance", perf)):
        print(f"{name} ({len(df.columns)} columns): " + ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields), flush=True)

    banner("build manifest")
    m = spark.table("lake.silver.build_manifest")
    if args.quarters:
        m = m.filter(F.col("source_quarter").isin(*args.quarters))
    mr = m.collect()
    check("every manifest row is ok and silver_rows == bronze_rows",
          bool(mr) and all(r["status"] == "ok" and r["silver_rows"] == r["bronze_rows"] for r in mr), f"({len(mr)} rows)")

    banner("loans")
    n = loans.count()
    check("loan_seq is unique", loans.select("loan_seq").distinct().count() == n, f"({n:,} loans)")
    left, detail = leftover_placeholders(loans, LOANS)
    check("no placeholder values left", left == 0, detail)
    mm = minmax(loans, LOANS, ("int", "dec", "date"))
    print("min / max per numeric or date column:", flush=True)
    for c, (lo, hi) in mm.items():
        print(f"    {c:28s} {lo}  ..  {hi}", flush=True)
    bounds = {"credit_score": (300, 850), "mi_pct": (0, 55), "num_units": (1, 4), "num_borrowers": (1, 10),
              "dti": (1, 65), "ltv": (1, 998), "cltv": (1, 998)}
    for c, (lo, hi) in bounds.items():
        a, b = mm[c]
        check(f"{c} within guide range {lo}..{hi}", a is not None and a >= lo and b <= hi, f"(actual {a}..{b})")

    # The guide gives no range for the loan term, only a formula (maturity - first payment + 1). Test that for every loan.
    formula = F.round(F.months_between("maturity_date", "first_pay_date")).cast("int") + 1
    bad_term = loans.filter(F.col("orig_term") != formula).count()
    long_term = loans.filter(F.col("orig_term") > 480).count()
    check("orig_term equals months(maturity - first payment) + 1 (Freddie's formula)", bad_term == 0,
          f"(mismatches: {bad_term}; loans above 480 months: {long_term}, kept as delivered)")

    banner("loan_performance")
    p = perf.count()
    print(f"rows: {p:,}", flush=True)
    left, detail = leftover_placeholders(perf, PERF)
    check("no placeholder values left", left == 0, detail)
    bad1 = perf.filter(F.col("delinq_status").isin("RA", "XX") & F.col("delinq_months").isNotNull()).count()
    bad2 = perf.filter(F.col("delinq_status").rlike(r"^\d+$") & F.col("delinq_months").isNull()).count()
    check("delinq_months is null exactly for RA / XX", bad1 == 0 and bad2 == 0, f"(RA/XX with a number: {bad1}, numeric status without number: {bad2})")
    mp = minmax(perf, PERF, ("date", "dec", "int", "delinq_months"))
    for c in ("report_period", "current_upb", "loan_age", "remaining_months", "eltv", "current_rate", "delinq_months"):
        print(f"    {c:28s} {mp[c][0]}  ..  {mp[c][1]}", flush=True)
    check("report_period between 2016-01 and 2026-12", str(mp["report_period"][0]) >= "2016-01-01" and str(mp["report_period"][1]) <= "2026-12-01",
          f"(actual {mp['report_period'][0]}..{mp['report_period'][1]})")
    check("current_upb never negative", mp["current_upb"][0] is not None and mp["current_upb"][0] >= 0, f"(min {mp['current_upb'][0]})")
    zb_bad = perf.filter(F.col("zero_bal_code").isNotNull()).join(ref, perf.zero_bal_code == ref.code, "left_anti").count()
    check("every zero_bal_code exists in ref_zero_balance_codes", zb_bad == 0, f"(unknown codes on {zb_bad} rows)")
    orphans = perf.select("loan_seq").distinct().join(loans.select("loan_seq"), "loan_seq", "left_anti").count()
    check("every performance loan exists in loans (no orphans)", orphans == 0, f"(orphans: {orphans})")

    banner("quality report")
    q = spark.table("lake.silver.quality_report")
    if args.quarters:
        q = q.filter(F.col("source_quarter").isin(*args.quarters))
    pf = q.agg(F.sum("parse_failures")).collect()[0][0] or 0
    check("no value failed to convert", pf == 0, f"(failed conversions: {pf})")
    print("columns with the most nulls in silver (share of rows):", flush=True)
    (q.groupBy("table_name", "column_name")
       .agg((F.sum("silver_null") / F.sum("row_count") * 100).alias("null_pct"), F.sum("sentinel_null").alias("placeholders_to_null"))
       .orderBy(F.desc("null_pct")).limit(10).show(truncate=False))

    banner("iceberg snapshots (history)")
    for t in ("lake.silver.loans", "lake.silver.loan_performance"):
        print(t, flush=True)
        spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 3").show(truncate=False)

    banner("sample rows")
    loans.limit(2).show(vertical=True, truncate=32)
    perf.filter(F.col("zero_bal_code").isNotNull()).limit(1).show(vertical=True, truncate=32)

    print(f"\nRESULT: {'ALL CHECKS PASSED' if not fails else 'FAILED: ' + '; '.join(fails)}", flush=True)
    spark.stop()
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
