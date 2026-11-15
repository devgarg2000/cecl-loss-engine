"""
Agent settings, loaded from the project's .env (never hardcoded, never printed).
Every other agent/ module reads settings from here, not from os.environ directly.
"""
import os

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(REPO_ROOT, ".env"))

LLM_BASE_URL = os.environ["AGENT_LLM_BASE_URL"]
LLM_API_KEY = os.environ["AGENT_LLM_API_KEY"]
LLM_MODEL = os.environ["AGENT_LLM_MODEL"]

DB_MODE = os.environ.get("AGENT_DB_MODE", "thrift")
THRIFT_HOST = os.environ.get("AGENT_THRIFT_HOST", "localhost")
THRIFT_PORT = int(os.environ.get("AGENT_THRIFT_PORT", "10000"))

AUDIT_LOG_PATH = os.path.join(REPO_ROOT, "agent", "logs", "audit.jsonl")

# How spark-submit jobs are run against the cluster (same pattern CLAUDE.md documents for every job).
SPARK_CONTAINER = "cecl-spark-master"
SPARK_SUBMIT_BASE = [
    "docker", "exec", "-u", "root", SPARK_CONTAINER,
    "spark-submit",
    "--master", "spark://spark-master:7077",
    "--executor-memory", "3g",
    "--conf", "spark.driver.host=spark-master",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
]

# The run-scenario tool never runs more than this many quarters in one confirmed action
# (a full 37-quarter run takes 77-90 minutes; this needs to stay demoable live).
MAX_QUARTERS_PER_RUN = 3
DEFAULT_RUN_QUARTERS = ["2023Q1"]
