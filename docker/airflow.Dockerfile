# Adds what the plain Airflow image is missing to submit Spark jobs directly over the
# network to spark://spark-master:7077 (SparkSubmitOperator), instead of docker-exec-ing
# into another container. Only used by airflow-scheduler, since that's the container that
# actually runs tasks under LocalExecutor.
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# --constraint pins the provider package to a version Airflow 2.9.3 actually supports. Without
# it, pip grabs the latest provider release (built for a newer Airflow major version) and it
# fails on import.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    apache-airflow-providers-apache-spark pyspark
# The constraints file pins pyspark 3.5.1, but the driver (this container) and the executors
# (the Spark cluster, bitnamilegacy/spark:3.5 = version 3.5.6) must match EXACTLY, or Spark
# refuses to deserialize tasks between them (InvalidClassException, confirmed by testing).
# Force the exact cluster version here, after the provider install is already satisfied.
RUN pip install --no-cache-dir pyspark==3.5.6
