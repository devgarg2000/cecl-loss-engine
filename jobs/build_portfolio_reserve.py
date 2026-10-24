"""
Build lake.gold.portfolio_reserve: one row per scenario, the final CECL number (step 6).

Just a rollup of lake.gold.sim_manifest (one row per scenario x origination quarter) into
one row per scenario, for whoever wants a single clean number per scenario without digging
into the manifest.

    spark-submit ... /opt/spark-jobs/build_portfolio_reserve.py
"""
import time

from pyspark.sql import functions as F

from lake import build_spark

TABLE = "lake.gold.portfolio_reserve"


def main():
    started = time.time()
    spark = build_spark("build_portfolio_reserve")

    manifest = spark.table("lake.gold.sim_manifest").filter(F.col("status") == "ok")
    rollup = (manifest.groupBy("scenario")
                      .agg(F.max("as_of_month").alias("as_of_month"),
                           F.sum("loans_written").alias("loan_count"),
                           F.sum("input_balance").alias("total_balance"),
                           F.sum("expected_loss").alias("expected_loss"),
                           F.count("*").alias("quarters_included"))
                      .withColumn("loss_rate_pct", F.round(F.col("expected_loss") / F.col("total_balance") * 100, 4))
                      .withColumn("computed_at", F.current_timestamp()))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    rollup.writeTo(TABLE).using("iceberg").tableProperty("format-version", "2").createOrReplace()

    written = spark.table(TABLE)
    print(f"\nwritten {TABLE}: {written.count()} scenario(s) | {time.time() - started:.0f}s", flush=True)
    (written.select("scenario", "as_of_month", "loan_count", "quarters_included",
                    F.round(F.col("total_balance") / 1e9, 2).alias("balance_billion"),
                    F.round(F.col("expected_loss") / 1e6, 1).alias("expected_loss_million"), "loss_rate_pct")
            .orderBy("expected_loss").show(truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
