"""
Build the SILVER Iceberg tables from BRONZE (resumable, one quarter at a time).

  lake.silver.loans                  one row per loan            (from bronze/orig)
  lake.silver.loan_performance       one row per loan per month  (from bronze/perf)
  lake.silver.quality_report         what became null, and why, per column per quarter
  lake.silver.ref_zero_balance_codes small lookup of zero balance codes
  lake.silver.build_manifest         ledger: which quarters are built, from which bronze

For each (table, quarter) that is new or out of date:
  read bronze -> convert types and placeholders to null (silver_schema.py) -> count what
  changed (quality report) -> replace that quarter in Iceberg (atomic partition overwrite)
  -> verify silver rows == bronze rows -> write the manifest row LAST.

    spark-submit ... /opt/spark-jobs/build_silver.py [--dry-run] [--force]
                     [--tables loans loan_performance] [--quarters 2023Q1 ..] [--years 2023 ..]
"""
import argparse
import datetime as dt
import sys
import time

from pyspark.sql import functions as F

from ingest_bronze import read_manifest as read_bronze_manifest
from lake import BRONZE_URI, build_spark
from silver_schema import TABLES, TRANSFORM_VERSION, ZERO_BAL_CODES, ddl_columns, expr

NS = "lake.silver"
MANIFEST_T = f"{NS}.build_manifest"
QUALITY_T = f"{NS}.quality_report"
REF_ZB_T = f"{NS}.ref_zero_balance_codes"


# ---------------------------------------------------------------- tables
def ensure_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    for short, (_kind, table, spec, order) in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (\n  {ddl_columns(spec)}\n) "
                  f"USING iceberg PARTITIONED BY (source_quarter) "
                  f"TBLPROPERTIES ('format-version'='2')")
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {order}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MANIFEST_T} (
        table_name string, source_quarter string, bronze_rows bigint, silver_rows bigint,
        bronze_ingested_at string, bronze_layout_version string, transform_version string,
        status string, seconds double, built_at string) USING iceberg""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {QUALITY_T} (
        table_name string, source_quarter string, column_name string, row_count bigint,
        bronze_null bigint, sentinel_null bigint, parse_failures bigint, silver_null bigint,
        built_at string) USING iceberg""")
    # tiny reference table: rebuilt on every run
    spark.sql(f"""CREATE OR REPLACE TABLE {REF_ZB_T} (
        code string COMMENT 'Zero balance code as it appears in loan_performance.zero_bal_code',
        description string COMMENT 'Meaning, from the Freddie Mac user guide',
        actual_loss_calculated boolean COMMENT 'True if Freddie calculates an actual loss for this code',
        priority int COMMENT 'If several events happen in one month the lowest number is reported')
        USING iceberg""")
    spark.createDataFrame(ZERO_BAL_CODES, "code string, description string, actual_loss_calculated boolean, priority int") \
         .writeTo(REF_ZB_T).append()


def read_silver_manifest(spark):
    return {(r["table_name"], r["source_quarter"]): r.asDict() for r in spark.table(MANIFEST_T).collect()}


def needs_work(short, kind, q, bronze_m, silver_m, force):
    b = bronze_m.get((kind, q))
    if b is None or b["status"] != "ok":
        return None, "bronze not ready"
    if force:
        return b, "forced"
    s = silver_m.get((short, q))
    if s is None:
        return b, "new"
    if s["status"] != "ok":
        return b, "previous attempt failed"
    if s["transform_version"] != TRANSFORM_VERSION:
        return b, f"transform changed ({s['transform_version']} -> {TRANSFORM_VERSION})"
    if s["bronze_ingested_at"] != b["ingested_at"] or s["bronze_layout_version"] != b["layout_version"]:
        return b, "bronze changed"
    return b, None


# ---------------------------------------------------------------- one (table, quarter)
def typed_frame(bronze, spec, q):
    return bronze.select(*[expr(c).alias(c.name) for c in spec], F.lit(q).alias("source_quarter"))


def quality_rows(bronze, spec, short, q, built_at):
    """Per column: how many nulls existed in bronze, how many placeholders became null, how many failed to convert."""
    aggs = [F.count(F.lit(1)).alias("n_rows")]
    cols = [c for c in spec if c.from_bronze]
    for c in cols:
        raw, typed = F.col(c.name), expr(c)
        sent = raw.isin(*c.null_if) if c.null_if else F.lit(False)
        aggs += [F.sum(raw.isNull().cast("long")).alias(f"{c.name}__bn"),
                 F.sum((raw.isNotNull() & sent).cast("long")).alias(f"{c.name}__sn"),
                 F.sum((raw.isNotNull() & ~sent & typed.isNull()).cast("long")).alias(f"{c.name}__pf")]
    r = bronze.agg(*aggs).collect()[0]
    out, total_sentinel, total_parse_fail = [], 0, 0
    for c in cols:
        bn, sn, pf = (r[f"{c.name}__{k}"] or 0 for k in ("bn", "sn", "pf"))
        out.append((short, q, c.name, r["n_rows"], bn, sn, pf, bn + sn + pf, built_at))
        total_sentinel += sn
        total_parse_fail += pf
    return out, total_sentinel, total_parse_fail


def build_one(spark, short, q, bronze_meta):
    kind, table, spec, _order = TABLES[short]
    t0 = time.time()
    built_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    bronze = spark.read.parquet(f"{BRONZE_URI}/{kind}/source_quarter={q}")

    q_rows, sentinel_total, parse_failures = quality_rows(bronze, spec, short, q, built_at)
    typed_frame(bronze, spec, q).writeTo(table).overwritePartitions()

    bronze_rows = bronze_meta["bronze_rows"]
    silver_rows = spark.table(table).filter(F.col("source_quarter") == q).count()
    status = "ok" if silver_rows == bronze_rows else "failed"
    seconds = round(time.time() - t0, 1)

    # quality report and manifest are written LAST (replace this table+quarter only)
    schema_q = ("table_name string, source_quarter string, column_name string, row_count bigint, bronze_null bigint, "
                "sentinel_null bigint, parse_failures bigint, silver_null bigint, built_at string")
    spark.sql(f"DELETE FROM {QUALITY_T} WHERE table_name = '{short}' AND source_quarter = '{q}'")
    spark.createDataFrame(q_rows, schema_q).writeTo(QUALITY_T).append()
    spark.sql(f"DELETE FROM {MANIFEST_T} WHERE table_name = '{short}' AND source_quarter = '{q}'")
    spark.createDataFrame([(short, q, bronze_rows, silver_rows, bronze_meta["ingested_at"],
                            bronze_meta["layout_version"], TRANSFORM_VERSION, status, seconds, built_at)],
                          "table_name string, source_quarter string, bronze_rows bigint, silver_rows bigint, "
                          "bronze_ingested_at string, bronze_layout_version string, transform_version string, "
                          "status string, seconds double, built_at string").writeTo(MANIFEST_T).append()

    print(f"[{short}] {q}: {status}  bronze={bronze_rows:,} silver={silver_rows:,}  "
          f"placeholders->null={sentinel_total:,}  failed conversions={parse_failures:,}  {seconds}s", flush=True)
    return status == "ok"


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", default=list(TABLES), choices=list(TABLES))
    ap.add_argument("--quarters", nargs="*")
    ap.add_argument("--years", nargs="*")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    spark = build_spark("build_silver")
    if not args.dry_run:                       # a dry run must change nothing, so it creates no tables
        ensure_tables(spark)
    bronze_m = read_bronze_manifest(spark)
    silver_m = read_silver_manifest(spark) if spark.catalog.tableExists(MANIFEST_T) else {}

    quarters = sorted({q for (_k, q) in bronze_m})
    if args.quarters:
        quarters = [q for q in quarters if q in args.quarters]
    if args.years:
        quarters = [q for q in quarters if q[:4] in args.years]

    plan, not_ready = [], []
    for q in quarters:
        for short in args.tables:
            kind = TABLES[short][0]
            meta, why = needs_work(short, kind, q, bronze_m, silver_m, args.force)
            if meta is None:
                not_ready.append((short, q))
            elif why:
                plan.append((short, q, meta, why))
    total = len(quarters) * len(args.tables)
    print(f"Quarters considered: {len(quarters)} | tasks: {total} | already done: {total - len(plan) - len(not_ready)} "
          f"| to do: {len(plan)} | bronze not ready: {len(not_ready)}", flush=True)
    for short, q, _m, why in plan:
        print(f"  will build {short} {q}  ({why})", flush=True)

    if args.dry_run or not plan:
        print("Dry run, nothing built." if args.dry_run else "Nothing to do.")
        spark.stop()
        return

    t0, failures = time.time(), 0
    for short, q, meta, _why in plan:                    # loans before loan_performance within a quarter
        try:
            if not build_one(spark, short, q, meta):
                failures += 1
        except Exception as e:
            failures += 1
            print(f"[{short}] {q}: ERROR {type(e).__name__}: {str(e)[:400]}", flush=True)
    print(f"Finished {len(plan)} task(s) in {time.time() - t0:.0f}s, {failures} failure(s)", flush=True)
    spark.stop()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
