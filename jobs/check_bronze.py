"""
Alignment check: are bronze columns in the right place?

Takes a few random bronze rows for one quarter, rebuilds each into the original
pipe-delimited line, and looks for that exact line in the raw text file.
If every rebuilt line exists in the source, no column was shifted or altered.

    spark-submit ... /opt/spark-jobs/check_bronze.py 2023Q1 [sample_size]
"""
import sys

from ingest_bronze import BRONZE_URI, LAYOUT, build_spark, find_files

quarter = sys.argv[1]
sample_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5

spark = build_spark()
all_ok = True

for kind, (names, drop) in LAYOUT.items():
    cols = [n for n in names if n not in drop]
    df = spark.read.parquet(f"{BRONZE_URI}/{kind}/source_quarter={quarter}")
    rows = df.sample(False, 0.001, seed=7).limit(sample_size).collect()

    # Rebuild the original line: empty strings were read as null, so put them back.
    wanted = {}
    for r in rows:
        line = "|".join("" if r[c] is None else r[c] for c in cols)
        if drop:                       # the dropped always-empty column = trailing "|"
            line += "|" * len(drop)
        wanted[line] = r

    found = set()
    with open(find_files(kind)[quarter], "r", encoding="utf-8", errors="replace") as f:
        for src in f:
            src = src.rstrip("\n")
            if src in wanted:
                found.add(src)

    ok = len(found) == len(wanted)
    all_ok &= ok
    print(f"[{kind}] {quarter}: {len(found)}/{len(wanted)} sampled rows found verbatim in source -> "
          f"{'ALIGNED' if ok else 'MISMATCH'}")
    sample = next(iter(wanted.values()), None)
    if sample is not None:
        print("  example row:", {c: sample[c] for c in cols[:8]}, "...")

spark.stop()
sys.exit(0 if all_ok else 1)
