"""
The real CECL pipeline, step 6. Manual trigger only (see docs/concepts-qa.md for why).

Starts from silver (already built separately — bronze/silver are a one-time load, not part of
this recurring cycle) and does everything a new quarter of data would need re-run:

  loan_current_state -> default_hazard -> loss_and_lgd -> reo_transition -> prepay_hazard
    -> simulate baseline -> adverse -> severely_adverse -> portfolio_reserve

Everything runs one task at a time, in that order — not in parallel. The cluster has one
worker. Running tasks "in parallel" from Airflow's point of view doesn't make Spark work
faster; it just means several separate driver processes are alive inside the scheduler
container at once, competing for memory. That's what happened the first time this DAG ran:
4 gold-layer tasks started together, 4 driver JVMs plus the Thrift server's own JVM filled
up the Docker VM's memory, and one task's executor was killed by the OS (exit code 137 =
out of memory), confirmed in the Spark worker's own logs.

Each task requests 4 of the worker's 6 cores and 4 of its 6 GB (SPARK_WORKER_MEMORY was
raised from 4G to 6G for the same reason — the old limit left no real room once the Thrift
server's 1 GB was subtracted). Thrift keeps running throughout; nothing needs stopping.

The simulation and rollup jobs are resumable (see build_simulation.py): if the hazard tables
didn't change, those tasks finish almost instantly instead of re-running for real.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JARS = ",".join(f"/opt/extra-jars/{name}" for name in [
    "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
    "hadoop-client-api-3.3.4.jar",
    "hadoop-client-runtime-3.3.4.jar",
])


def spark_task(task_id, script, extra_args=""):
    """One SparkSubmitOperator with the settings every job in this DAG shares."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"/opt/airflow/jobs/{script}",
        application_args=extra_args.split() if extra_args else None,
        name=f"airflow_{task_id}",
        jars=JARS,
        total_executor_cores=4,
        executor_memory="4g",
        conf={"spark.driver.host": "airflow-scheduler"},
    )


with DAG(
    dag_id="cecl_pipeline",
    description="CECL step 6: gold-layer rebuild, 3 scenarios, portfolio reserve rollup",
    schedule=None,                      # manual trigger only — see docs/concepts-qa.md
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cecl"],
) as dag:
    loan_current_state = spark_task("loan_current_state", "build_loan_current_state.py")
    default_hazard = spark_task("default_hazard", "build_default_hazard.py")
    loss_and_lgd = spark_task("loss_and_lgd", "build_loss_and_lgd.py")
    prepay_hazard = spark_task("prepay_hazard", "build_prepay_hazard.py")
    reo_transition = spark_task("reo_transition", "build_reo_transition.py")

    simulate_baseline = spark_task("simulate_baseline", "build_simulation.py", "--scenario baseline")
    simulate_adverse = spark_task("simulate_adverse", "build_simulation.py", "--scenario adverse")
    simulate_severely_adverse = spark_task("simulate_severely_adverse", "build_simulation.py", "--scenario severely_adverse")
    portfolio_reserve = spark_task("portfolio_reserve", "build_portfolio_reserve.py")

    (loan_current_state >> default_hazard >> loss_and_lgd >> reo_transition >> prepay_hazard
     >> simulate_baseline >> simulate_adverse >> simulate_severely_adverse >> portfolio_reserve)
