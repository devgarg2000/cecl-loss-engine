# CECL Loss Simulation Engine — Platform Scaffold

Portfolio-wide expected credit loss simulation on Freddie Mac loan-level data,
built on an open-source, cloud-portable stack: MinIO (S3-compatible lake),
Spark (distributed compute), Airflow (orchestration).

## What's here

```
cecl-loss-engine/
├── docker-compose.yml   # the whole platform: MinIO, Postgres, Airflow, Spark
├── .env                 # MinIO credentials (change before going further)
├── dags/                # Airflow DAGs go here
├── jobs/                # PySpark jobs go here (mounted into both Airflow and Spark)
└── data/                # local scratch space (gitignored) — raw/bronze/silver/gold
```

## First-time setup

1. Open a terminal in this folder.
2. Start everything:
   ```
   docker compose up -d
   ```
   First run pulls several images (Airflow, Spark, Postgres, MinIO) — this can
   take a few minutes. Subsequent starts are fast.
3. Check everything came up healthy:
   ```
   docker compose ps
   ```
   All services should show `running` (airflow-init and createbuckets will
   show `exited (0)` — that's correct, they're one-shot setup jobs).

## Where to look once it's running

| Service | URL | Login |
|---|---|---|
| MinIO console | http://localhost:9001 | login is in your local `.env` |
| Airflow UI | http://localhost:8080 | login is in your local `.env` |
| Spark master UI | http://localhost:8081 | — |

In the MinIO console you should already see a `cecl-lake` bucket with
`bronze`, `silver`, and `gold` folders — created automatically on first
startup.

## Stopping / resetting

```
docker compose down          # stop everything, keep data
docker compose down -v       # stop everything AND wipe all volumes (fresh start)
```

## Next step

Add the Freddie Mac ingestion job under `jobs/` and its Airflow DAG under
`dags/` — that's the bronze-layer ingestion step in the build plan.
