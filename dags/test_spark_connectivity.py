"""
One-task test DAG: proves Airflow can submit a real job straight to the Spark cluster over
the network (spark://spark-master:7077), not by exec-ing into another container.

Runs build_portfolio_reserve.py, a small, safe, idempotent job (just re-reads sim_manifest and
rewrites one small table). Manual trigger only.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="test_spark_connectivity",
    description="One-off test: can Airflow submit a job straight to the Spark cluster?",
    schedule=None,                      # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cecl", "test"],
) as dag:
    run_portfolio_reserve = SparkSubmitOperator(
        task_id="build_portfolio_reserve",
        conn_id="spark_default",        # set via AIRFLOW_CONN_SPARK_DEFAULT in docker-compose.yml
        application="/opt/airflow/jobs/build_portfolio_reserve.py",
        name="airflow_test_portfolio_reserve",
        # Iceberg (table format) + the S3A connector jars (talks to MinIO) — pip's pyspark doesn't
        # bundle any of these; the bitnami Spark image we use for the cluster does, built in.
        jars=",".join([
            "/opt/extra-jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
            "/opt/extra-jars/hadoop-aws-3.3.4.jar",
            "/opt/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/extra-jars/hadoop-client-api-3.3.4.jar",
            "/opt/extra-jars/hadoop-client-runtime-3.3.4.jar",
        ]),
        executor_memory="2g",
        total_executor_cores=2,
        conf={"spark.driver.host": "airflow-scheduler"},  # workers connect back to this container
        verbose=True,
    )
