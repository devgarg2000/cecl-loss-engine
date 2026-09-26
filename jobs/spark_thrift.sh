#!/usr/bin/env bash
# Starts Spark's Thrift server (JDBC endpoint on port 10000) as a long-lived
# Spark application, for SQL clients such as DBeaver.
# Kept deliberately small (2 cores, 1 GB executor) so batch jobs still get the rest
# of the worker (4 cores, 3 GB). Run by the `spark-thrift` service in docker-compose.
exec /opt/bitnami/spark/bin/spark-submit \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --name spark-thrift \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --executor-memory 1g \
  --driver-memory 512m \
  --conf spark.driver.host=spark-thrift \
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
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  spark-internal
