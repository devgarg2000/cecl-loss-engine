"""
Shared Spark setup for every job: MinIO (S3A) + the Iceberg catalog.

Bronze stays plain Parquet under s3a://cecl-lake/bronze/.
Silver and gold are Iceberg tables in the catalog named `lake`:
    lake.silver.loans  ->  s3a://cecl-lake/silver/loans
    lake.gold.<table>  ->  s3a://cecl-lake/gold/<table>

Known limitation: the Hadoop catalog commits by renaming files, which S3-style
storage does not do atomically. That is fine for one writer at a time (our case).
If we ever need concurrent writers, switch this to Iceberg's JDBC catalog on
the existing Postgres.
"""
import os

from pyspark.sql import SparkSession

BRONZE_URI = "s3a://cecl-lake/bronze"
WAREHOUSE = "s3a://cecl-lake"
CATALOG = "lake"


def build_spark(app_name="cecl"):
    spark = (
        SparkSession.builder.appName(app_name)
        # MinIO connection (credentials come from the container environment)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Re-running a bronze quarter replaces only that quarter's folder
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Iceberg: SQL extensions + a catalog called `lake` stored in MinIO
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # Iceberg's Arrow "vectorized" reader crashed the executor JVMs (SIGSEGV in libjvm, exit code 134)
        # when reading lake.silver.loan_performance. Turning it off avoids the crash. Root cause not proven.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
