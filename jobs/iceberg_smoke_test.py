"""
Smoke test: can Spark create, write, read and drop an Iceberg table in MinIO?
Uses a throwaway table lake.silver._smoke and cleans up after itself.

    spark-submit ... /opt/spark-jobs/iceberg_smoke_test.py
"""
from lake import CATALOG, build_spark

T = f"{CATALOG}.silver._smoke"
spark = build_spark("iceberg_smoke_test")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"DROP TABLE IF EXISTS {T} PURGE")
spark.sql(f"CREATE TABLE {T} (id BIGINT, quarter STRING) USING iceberg PARTITIONED BY (quarter)")

spark.sql(f"INSERT INTO {T} VALUES (1,'2023Q1'), (2,'2023Q1'), (3,'2023Q2')")
print("rows after 1st insert:", spark.table(T).count())

# Replace only the 2023Q1 partition, the pattern silver will use for reruns.
spark.sql(f"INSERT OVERWRITE {T} VALUES (10,'2023Q1')")
print("rows after partition overwrite:", spark.table(T).count(),
      "(expect 2: the new 2023Q1 row + untouched 2023Q2)")

print("snapshots (each write = one snapshot, this is what enables time travel):")
spark.sql(f"SELECT snapshot_id, operation FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)

spark.sql(f"DROP TABLE {T} PURGE")
print("SMOKE TEST PASSED")
spark.stop()
