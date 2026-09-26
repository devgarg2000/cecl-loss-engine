"""
Pre-checks on BRONZE before building silver. READ-ONLY.

  1. Is (loan_seq, report_period) unique in every quarter of bronze.perf?   (duplicate months)
  2. Does every loan in bronze.perf exist in bronze.orig?                   (orphans)
  3. Which orig loans have a loan_seq whose embedded quarter differs from the file's quarter?

    spark-submit ... /opt/spark-jobs/precheck_silver.py
"""
import time

from pyspark.sql import functions as F

from lake import BRONZE_URI, build_spark

spark = build_spark("precheck_silver")
orig = spark.read.parquet(f"{BRONZE_URI}/orig")            # includes the source_quarter partition column
quarters = sorted(r[0] for r in orig.select("source_quarter").distinct().collect())
print(f"quarters in bronze: {len(quarters)}", flush=True)

# ---- 1. duplicate (loan_seq, report_period), one quarter at a time ----
print("\n=== 1. duplicate months per quarter (expect 0) ===", flush=True)
total_rows = total_dups = 0
t0 = time.time()
for q in quarters:
    p = spark.read.parquet(f"{BRONZE_URI}/perf/source_quarter={q}").select("loan_seq", "report_period")
    n = p.count()
    dups = p.groupBy("loan_seq", "report_period").count().filter("count > 1").count()
    total_rows += n
    total_dups += dups
    print(f"{q}: perf_rows={n:,} duplicate_keys={dups:,}  ({time.time() - t0:.0f}s so far)", flush=True)
print(f"TOTAL perf rows={total_rows:,}  duplicate keys={total_dups:,}", flush=True)

# ---- 2. orphans: loans in perf that are not in orig ----
print("\n=== 2. orphan loans ===", flush=True)
orig_ids = orig.select("loan_seq")
perf_ids = spark.read.parquet(f"{BRONZE_URI}/perf").select("loan_seq").distinct()
n_perf_loans = perf_ids.count()
orphans = perf_ids.join(orig_ids, "loan_seq", "left_anti")
n_orphans = orphans.count()
print(f"distinct loans in perf: {n_perf_loans:,} | not present in orig: {n_orphans:,}", flush=True)
if n_orphans:
    print("examples:", [r[0] for r in orphans.limit(5).collect()], flush=True)
no_perf = orig_ids.join(perf_ids, "loan_seq", "left_anti").count()
print(f"orig loans with no performance rows at all: {no_perf:,}", flush=True)

# ---- 3. loan ids whose embedded quarter differs from the file's quarter ----
print("\n=== 3. orig loans whose loan_seq quarter differs from their file's quarter ===", flush=True)
mm = (orig.filter(F.substring("loan_seq", 2, 4) != F.substring("source_quarter", 3, 4))
          .select("loan_seq", "source_quarter", "first_pay_date", "maturity_date", "orig_upb", "state", "seller")
          .collect())
print(f"count: {len(mm)}", flush=True)
for r in mm:
    print("  ", r.asDict(), flush=True)

spark.stop()
