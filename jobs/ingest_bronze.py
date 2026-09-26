"""
Bronze ingestion (resumable): Freddie Mac raw text files -> Parquet in MinIO.

Every run works out what is NEW and only does that:
  * A quarter is "done" once a manifest row (status=ok) exists for it AND the
    source file's size/mtime still match what was recorded.
  * The manifest row is written LAST, after the row count has been verified.
    A crash mid-quarter therefore leaves no manifest row, and the next run
    redoes that quarter (overwrite replaces the half-written folder).
  * Files still being downloaded/extracted are skipped: a file must have the
    same size before and after a short wait, and both orig+perf must exist.

Values are never changed (every column stays a string); typing/cleaning is silver.

Output:
  s3a://cecl-lake/bronze/{orig,perf}/source_quarter=<Q>/...
  s3a://cecl-lake/bronze/_manifest/kind=<k>/source_quarter=<Q>/...

Flags:  --dry-run   show the plan, change nothing
        --force     redo the selected quarters even if already done
        --years / --quarters   limit what is considered
"""
import argparse
import datetime as dt
import glob
import os
import re
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)
from pyspark.sql.utils import AnalysisException

from columns import LAYOUT, LAYOUT_VERSION   # every column name lives in columns.py
from lake import BRONZE_URI, build_spark      # shared MinIO + Iceberg setup

RAW_DIR = "/opt/spark-data/raw"          # data/raw/ mounted by docker-compose
MANIFEST_URI = f"{BRONZE_URI}/_manifest"
KINDS = ("orig", "perf")

MANIFEST_SCHEMA = StructType([
    StructField("kind", StringType()),
    StructField("source_quarter", StringType()),
    StructField("size_bytes", LongType()),
    StructField("mtime", LongType()),
    StructField("source_rows", LongType()),
    StructField("bronze_rows", LongType()),
    StructField("status", StringType()),      # ok | failed
    StructField("seconds", DoubleType()),
    StructField("ingested_at", StringType()),
    StructField("layout_version", StringType()),   # which columns.py layout wrote this
])


# ---------- discovering what is on disk ----------

def find_files(kind):
    """{quarter: path} for every <kind>_YYYYQn.txt under RAW_DIR."""
    found = {}
    for path in glob.glob(f"{RAW_DIR}/**/{kind}_*.txt", recursive=True):
        m = re.search(rf"{kind}_(\d{{4}}Q[1-4])\.txt$", path)
        if m:
            found[m.group(1)] = path
    return found


def stat_of(path):
    st = os.stat(path)
    return st.st_size, int(st.st_mtime)


def count_lines(path):
    """Independent line count of the source file (cheap: raw byte scan)."""
    n, last = 0, b"\n"
    with open(path, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            n += chunk.count(b"\n")
            last = chunk[-1:]
    return n + (0 if last == b"\n" else 1)


def ready_quarters(files_by_kind, settle_seconds):
    """Quarters where BOTH files exist and stopped growing during a short wait."""
    both = sorted(set(files_by_kind["orig"]) & set(files_by_kind["perf"]))
    before = {(k, q): stat_of(files_by_kind[k][q]) for q in both for k in KINDS}
    time.sleep(settle_seconds)
    ready, not_ready = [], []
    for q in both:
        ok = all(before[(k, q)] == stat_of(files_by_kind[k][q]) and before[(k, q)][0] > 0
                 for k in KINDS)
        (ready if ok else not_ready).append(q)
    unpaired = sorted(set(files_by_kind["orig"]) ^ set(files_by_kind["perf"]))
    return ready, not_ready + unpaired


# ---------- the manifest ----------

def read_manifest(spark):
    """{(kind, quarter): row-as-dict}; empty on the very first run."""
    try:
        # mergeSchema: older manifest rows were written before layout_version existed
        rows = spark.read.option("mergeSchema", "true").parquet(MANIFEST_URI).collect()
    except AnalysisException:
        return {}
    return {(r["kind"], r["source_quarter"]): r.asDict() for r in rows}


def write_manifest_row(spark, row):
    (spark.createDataFrame([row], MANIFEST_SCHEMA)
          .write.mode("overwrite").partitionBy("kind", "source_quarter")
          .parquet(MANIFEST_URI))


def needs_work(manifest, kind, quarter, path, force):
    if force:
        return "forced"
    m = manifest.get((kind, quarter))
    if m is None:
        return "new"
    if m["status"] != "ok":
        return "previous attempt failed"
    if m.get("layout_version") != LAYOUT_VERSION:
        return f"column layout changed ({m.get('layout_version')} -> {LAYOUT_VERSION})"
    size, mtime = stat_of(path)
    if (m["size_bytes"], m["mtime"]) != (size, mtime):
        return "source file changed"
    return None


# ---------- doing the work ----------

def ingest_one(spark, kind, quarter, path):
    names, drop = LAYOUT[kind]
    schema = StructType([StructField(n, StringType(), True) for n in names])
    size, mtime = stat_of(path)
    t0 = time.time()

    source_rows = count_lines(path)
    df = (spark.read.csv(f"file://{path}", sep="|", header=False, schema=schema)
               .drop(*drop)
               .withColumn("source_quarter", F.lit(quarter)))
    df.write.mode("overwrite").partitionBy("source_quarter").parquet(f"{BRONZE_URI}/{kind}")

    # Read the quarter back from MinIO (Parquet footers, so this is fast).
    bronze_rows = spark.read.parquet(f"{BRONZE_URI}/{kind}/source_quarter={quarter}").count()
    status = "ok" if bronze_rows == source_rows else "failed"
    row = (kind, quarter, size, mtime, source_rows, bronze_rows, status,
           round(time.time() - t0, 1), dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
           LAYOUT_VERSION)
    write_manifest_row(spark, row)     # written LAST
    print(f"[{kind}] {quarter}: {status}  source={source_rows:,} bronze={bronze_rows:,}  {row[7]}s")
    return status == "ok"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarters", nargs="*", help="e.g. 2023Q1 2023Q2")
    ap.add_argument("--years", nargs="*", help="e.g. 2017 2018")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--settle-seconds", type=int, default=15,
                    help="wait between two size checks to detect files still being written")
    args = ap.parse_args()

    files = {k: find_files(k) for k in KINDS}
    ready, skipped = ready_quarters(files, args.settle_seconds)
    if args.quarters:
        ready = [q for q in ready if q in args.quarters]
    if args.years:
        ready = [q for q in ready if q[:4] in args.years]
    if skipped:
        print(f"Not ready (missing pair or still changing), skipped: {skipped}")

    spark = build_spark("ingest_bronze")
    manifest = read_manifest(spark)

    plan = []
    for q in ready:
        for k in KINDS:
            why = needs_work(manifest, k, q, files[k][q], args.force)
            if why:
                plan.append((k, q, why))
    done = len(ready) * len(KINDS) - len(plan)
    print(f"Ready quarters: {len(ready)} | already done: {done} | to do: {len(plan)}")
    for k, q, why in plan:
        print(f"  will ingest {k} {q}  ({why})")

    if args.dry_run or not plan:
        print("Dry run, nothing changed." if args.dry_run else "Nothing to do.")
        spark.stop()
        return

    t0 = time.time()
    failures = 0
    for k, q, _ in plan:
        try:
            if not ingest_one(spark, k, q, files[k][q]):
                failures += 1
        except Exception as e:                      # keep going; report at the end
            failures += 1
            print(f"[{k}] {q}: ERROR {type(e).__name__}: {str(e)[:300]}")
    print(f"Finished {len(plan)} task(s) in {time.time() - t0:.0f}s, {failures} failure(s)")
    spark.stop()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
