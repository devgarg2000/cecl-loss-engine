#!/usr/bin/env bash
# Spark SQL shell against the lake (MinIO + Iceberg catalog `lake`).
# Run from Windows, inside the Spark master container:
#   interactive:  docker exec -it -u root cecl-spark-master bash /opt/spark-jobs/spark_sql.sh
#   one query:    docker exec -u root cecl-spark-master bash /opt/spark-jobs/spark_sql.sh -e "SELECT 1"
# Credentials come from the container's environment (set by docker-compose from .env).
exec spark-sql \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --executor-memory 1g \
  --conf spark.driver.host=spark-master \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key="$MINIO_ROOT_USER" \
  --conf spark.hadoop.fs.s3a.secret.key="$MINIO_ROOT_PASSWORD" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=s3a://cecl-lake \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  -i /opt/spark-jobs/sql/init.sql \
  "$@"
