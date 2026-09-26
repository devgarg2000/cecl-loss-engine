"""
Pre-flight audit of the RAW Freddie Mac text files. READ-ONLY: writes nothing.

Step 1 (every line): field counts, line endings, non-ASCII text, blank lines.
Step 2 (random sample, default 2% of rows): for each column, how often it is filled per
        quarter, whether the values look like what the column name promises, and which
        values appear when there are only a few.
Also: orig loan_seq is unique, and the quarter inside loan_seq matches the file's quarter.

    spark-submit ... /opt/spark-jobs/profile_raw.py [--sample 0.02] [--kinds orig perf]

A sample can miss a very rare bad value. It will not miss a shifted column or a wrong meaning.
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import functions as F

from columns import ORIG_COLUMNS, PERF_COLUMNS
from ingest_bronze import find_files
from lake import build_spark

NAMES = {"orig": ORIG_COLUMNS, "perf": PERF_COLUMNS}   # perf has 35 columns incl. undocumented_amount
EXPECTED_PIPES = {"orig": 30, "perf": 34}            # 31 / 35 fields

LOANSEQ = r"^[FA]\d{2}Q[1-4]\d{7}$"
DATE = r"^\d{6}$"
DEC = r"^-?\d+\.\d{2}$"

PATTERNS = {
    "orig": {
        "credit_score": r"^\d{3,4}$", "first_pay_date": DATE, "first_time_buyer": r"^[YN9]$",
        "maturity_date": DATE, "msa": r"^\d{5}$", "mi_pct": r"^\d{1,3}$", "num_units": r"^\d{1,2}$",
        "occupancy": r"^[PIS9]$", "cltv": r"^\d{1,3}$", "dti": r"^\d{1,3}$", "orig_upb": r"^\d+(\.\d+)?$",
        "ltv": r"^\d{1,3}$", "int_rate": r"^\d+(\.\d+)?$", "channel": r"^[RBCT9]$", "ppm_flag": r"^[YN]$",
        "amort_type": r"^(FRM|ARM)$", "state": r"^[A-Z]{2}$", "prop_type": r"^(CO|PU|MH|SF|CP|99)$",
        "zip3": r"^\d{3,5}$", "loan_seq": LOANSEQ, "purpose": r"^[PCNR9]$", "orig_term": r"^\d{1,3}$",
        "num_borrowers": r"^\d{1,2}$", "super_conforming_flag": r"^[YN]$",
        "pre_relief_refi_loan_seq": LOANSEQ, "special_eligibility_program": r"^[HFR9]$",
        "relief_refi_indicator": r"^[YN]$", "property_valuation_method": r"^\d$",
        "interest_only_indicator": r"^[YN]$", "mi_cancellation_indicator_orig": r"^9999$",
    },
    "perf": {
        "loan_seq": LOANSEQ, "report_period": DATE, "current_upb": r"^\d+\.\d{2}$",
        "delinq_status": r"^(\d{1,3}|RA|XX)$", "loan_age": r"^\d{1,3}$", "remaining_months": r"^-?\d{1,3}$",
        "defect_settlement_date": DATE, "modification_flag": r"^[YP]$", "zero_bal_code": r"^\d{2}$",
        "zero_bal_date": DATE, "current_rate": r"^\d+\.\d{3}$", "non_interest_bearing_upb": r"^\d+\.\d{2}$",
        "ddlpi": DATE, "mi_recoveries": DEC, "net_sale_proceeds": r"^(U|-?\d+\.\d{2})$",
        "non_mi_recoveries": DEC, "total_expenses": DEC, "legal_costs": DEC, "maintenance_costs": DEC,
        "taxes_insurance": DEC, "misc_expenses": DEC, "actual_loss": DEC, "cumulative_mod_cost": DEC,
        "rate_step_indicator": r"^[YN]$", "payment_deferral_flag": r"^[YPC]$", "eltv": r"^\d{1,3}$",
        "zero_bal_removal_upb": DEC, "delinquent_accrued_interest": DEC, "disaster_delinquency_flag": r"^Y$",
        "borrower_assistance_status": r"^[FRT]$", "current_month_mod_cost": DEC,
        "interest_bearing_upb": r"^\d+\.\d{2}$", "mi_cancellation_indicator": r"^[YN7]$",
        "undocumented_amount": r"^\d+\.\d{2}$",
    },
}


def banner(text):
    print("\n" + "=" * 8 + " " + text + " " + "=" * 8, flush=True)


def audit(spark, kind, frac, quarters=None, field35_only=False):
    names, expected = NAMES[kind], EXPECTED_PIPES[kind]
    files = find_files(kind)
    if quarters:
        files = {q: p for q, p in files.items() if q in quarters}
    lines = (spark.read.text([f"file://{p}" for p in files.values()])
                  .withColumn("quarter", F.regexp_extract(F.input_file_name(), r"_(\d{4}Q[1-4])\.txt", 1)))

    if field35_only:                      # quick mode: only the study of the 35th perf field
        if kind == "perf":
            parts = F.split("value", r"\|", -1)
            df = (lines.sample(False, frac, seed=42)
                       .select("quarter", *[parts[i].alias(n) for i, n in enumerate(names)]))
            explain_field_35(df)
        return

    # ---- Step 1: every line ----
    banner(f"{kind} STEP 1: every line of {len(files)} files")
    pipes = F.length("value") - F.length(F.regexp_replace("value", r"\|", ""))
    s1 = (lines.groupBy("quarter").agg(
            F.count("*").alias("rows"),
            F.sum((pipes != expected).cast("int")).alias("wrong_field_count"),
            F.sum(F.col("value").endswith("\r").cast("int")).alias("crlf_lines"),
            F.sum((F.octet_length("value") != F.length("value")).cast("int")).alias("non_ascii_lines"),
            F.sum((F.col("value") == "").cast("int")).alias("blank_lines"))
            .orderBy("quarter").collect())
    tot = {k: sum(r[k] for r in s1) for k in ("rows", "wrong_field_count", "crlf_lines", "non_ascii_lines", "blank_lines")}
    print(f"rows={tot['rows']:,}  wrong_field_count={tot['wrong_field_count']:,}  crlf_lines={tot['crlf_lines']:,}  "
          f"non_ascii_lines={tot['non_ascii_lines']:,}  blank_lines={tot['blank_lines']:,}")
    for r in s1:
        if r["wrong_field_count"] or r["crlf_lines"] or r["non_ascii_lines"] or r["blank_lines"]:
            print("  quarter with issues:", r.asDict())

    # ---- Step 2: sample ----
    banner(f"{kind} STEP 2: {frac:.0%} random sample, per column")
    parts = F.split("value", r"\|", -1)
    df = (lines.sample(False, frac, seed=42)
               .select("quarter", *[parts[i].alias(n) for i, n in enumerate(names)])
               .persist(StorageLevel.MEMORY_AND_DISK))
    n_sample = df.count()
    print(f"sample rows: {n_sample:,}")

    filled = {c: F.avg(F.when(F.col(c) != "", 1).otherwise(0)) for c in names}
    per_q = df.groupBy("quarter").agg(*[e.alias(c) for c, e in filled.items()]).collect()
    overall = df.agg(*[e.alias(c) for c, e in filled.items()],
                     *[F.approx_count_distinct(c).alias("d_" + c) for c in names]).collect()[0]
    pats = PATTERNS[kind]
    fails = df.agg(*[F.sum(((F.col(c) != "") & ~F.col(c).rlike(p)).cast("int")).alias(c)
                     for c, p in pats.items()]).collect()[0]

    print(f"{'column':32s} {'filled%':>8s} {'min%/q':>7s} {'max%/q':>7s} {'distinct~':>10s} {'bad-format':>10s}")
    for c in names:
        pcts = [(r[c] or 0.0, r["quarter"]) for r in per_q]
        lo, hi = min(pcts), max(pcts)
        bad = fails[c] if c in pats else "-"
        flag = "  <-- CHECK" if (c in pats and bad) else ""
        print(f"{c:32s} {overall[c]*100:8.2f} {lo[0]*100:7.1f} {hi[0]*100:7.1f} {overall['d_'+c]:10d} {bad!s:>10s}{flag}")

    for c, p in pats.items():
        if fails[c]:
            ex = (df.filter((F.col(c) != "") & ~F.col(c).rlike(p)).groupBy(c).count()
                    .orderBy(F.desc("count")).limit(6).collect())
            print(f"  bad values in {c}: " + ", ".join(f"{r[c]!r} x{r['count']}" for r in ex))

    banner(f"{kind} values of columns with few distinct values")
    for c in names:
        if overall["d_" + c] <= 40 and overall[c] > 0:
            top = df.filter(F.col(c) != "").groupBy(c).count().orderBy(F.desc("count")).limit(12).collect()
            print(f"  {c}: " + ", ".join(f"{r[c]}={r['count']}" for r in top))

    # loan id checks
    if kind == "orig":
        ids = lines.select(F.split("value", r"\|", -1)[19].alias("loan_seq"), "quarter")
        n, d = ids.count(), ids.select("loan_seq").distinct().count()
        print(f"\norig loan_seq: {n:,} rows, {d:,} distinct -> {'UNIQUE' if n == d else 'DUPLICATES: ' + str(n - d)}")
        mism = ids.filter(F.substring("loan_seq", 2, 4) != F.substring("quarter", 3, 4)).count()
        print(f"orig loan_seq whose embedded quarter differs from the file's quarter: {mism:,}")
    else:
        mism = df.filter(F.substring("loan_seq", 2, 4) != F.substring("quarter", 3, 4)).count()
        print(f"\nperf (sample) loan_seq whose embedded quarter differs from the file's quarter: {mism:,}")
        explain_field_35(df)
    df.unpersist()


def explain_field_35(df):
    """When is the undocumented 35th perf field filled? (sample). Shows % of rows filled per group."""
    banner("perf: when is undocumented_amount filled? (% of sampled rows, per group)")
    filled = (F.col("undocumented_amount") != "").cast("int")
    age = F.col("loan_age").cast("int")
    age_bucket = (F.when(age < 6, "a: age 0-5").when(age < 12, "b: age 6-11").when(age < 24, "c: age 12-23")
                   .when(age < 48, "d: age 24-47").otherwise("e: age 48+"))
    groups = {
        "loan age (months)": age_bucket,
        "report year": F.substring("report_period", 1, 4),
        "modification_flag": F.when(F.col("modification_flag") == "", "(empty)").otherwise(F.col("modification_flag")),
        "servicer": F.col("servicer_name"),
        "report month, 2024 onward": F.when(F.col("report_period") >= "202401", F.col("report_period")).otherwise("(before 2024)"),
        "has zero_bal_code (loan ended)": (F.col("zero_bal_code") != "").cast("string"),
        "delinquency status": F.when(F.col("delinq_status") == "00", "00 current")
                               .when(F.col("delinq_status").isin("XX", "RA"), F.col("delinq_status"))
                               .otherwise("late (01-99)"),
        "current_upb == interest_bearing_upb": (F.col("current_upb") == F.col("interest_bearing_upb")).cast("string"),
    }
    for label, expr in groups.items():
        print(f"\n-- filled % by {label}")
        (df.groupBy(expr.alias("group")).agg(F.count("*").alias("rows"), F.round(F.avg(filled) * 100, 1).alias("filled_pct"))
           .orderBy("group").show(70, truncate=False))
    print("\n-- distinct values seen in the field:")
    df.filter(F.col("undocumented_amount") != "").groupBy("undocumented_amount").count().show(10, truncate=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=0.02)
    ap.add_argument("--kinds", nargs="*", default=["orig", "perf"])
    ap.add_argument("--quarters", nargs="*", help="only these quarters, e.g. 2017Q1 2024Q1")
    ap.add_argument("--field35-only", action="store_true", help="only study the undocumented 35th perf field")
    args = ap.parse_args()
    spark = build_spark("profile_raw")
    for kind in args.kinds:
        audit(spark, kind, args.sample, args.quarters, args.field35_only)
    spark.stop()


if __name__ == "__main__":
    main()
