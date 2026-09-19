## Rules

### Always
- Explain what a command does before running it
- Ask before deleting anything in data/ or dropping a Docker volume
- Keep changes scoped to what was asked — don't refactor unrelated code
- Keep code simple and humanised so it is easy to understand, variable names and others should be comprehensive. And code should be reusable.
- As iam learning scaling, so keep me posted for each and every step. I dont want to you to by-pass anything. Each command you run on terminal has to proper and traditional and not claude format command so i can understand the command.

### Never
- Never commit .env or anything with credentials
- Never run destructive commands (rm -rf, force push, docker compose down -v) without asking first
- Never modify docker-compose.yml service definitions without explaining the change first


# CECL Loss Simulation Engine — Project Context

## Who I am / why this project exists

I'm Dev, a Data Engineer (~3.5 yrs experience) at AllianceBernstein (asset
management), core stack PySpark/AWS/Snowflake. I'm building this as a
portfolio-grade system to prove I can build at scale — for my resume,
LinkedIn, and interviews. This is deliberately NOT a Kaggle-style prediction
notebook. Those are saturated on GitHub. This is a real infrastructure/
systems build.

## The problem, in plain terms

Banks holding millions of mortgages must estimate, every quarter, how much
money they'll lose if the economy turns bad — under the CECL accounting
standard. This means simulating the ENTIRE remaining life of every loan,
month by month, under multiple economic scenarios (baseline / adverse /
severely adverse), and rolling it up to one portfolio-level number.

**Why it's a genuine scale problem (not just "big data for its own sake")**:
55M loans × ~3 scenarios × up to 360 remaining months each = billions of
simulation cells, and it has to be recomputed on a deadline every quarter
when forecasts change. The difficulty scales combinatorially with scenario
count — that's the actual engineering challenge, not a bigger CSV.

## Dataset

Freddie Mac Single-Family Loan-Level Dataset — ~55M loans, monthly
performance history, free for non-commercial/academic use.
https://freddiemac.com/research/datasets/sf-loanlevel-dataset

## Architecture (open-source, cloud-portable — runs anywhere, no AWS bill)

- **MinIO** — S3-compatible object storage (the lake). Bronze/silver/gold
  medallion layout.
- **Spark** — distributed compute, both the simulation logic and eventual
  streaming work on other planned modules.
- **Airflow** — orchestration (LocalExecutor, backed by Postgres).
- **Postgres** — Airflow's metadata DB only (not for the loan data itself).
- Everything runs via a single `docker-compose.yml` — no cloud account
  needed for local development.

## Current status (as of this handover)

The platform scaffold is written and the person has downloaded/moved it to:
`C:\Users\dev.garg\Desktop\ScalableProjects\cecl-loss-engine\cecl-loss-engine\`

Folder structure:
```
cecl-loss-engine/
├── docker-compose.yml   # MinIO, Postgres, Airflow, Spark cluster
├── .env                 # MinIO credentials
├── .gitignore
├── README.md
├── dags/                # Airflow DAGs (empty so far)
├── jobs/                # PySpark jobs (empty so far)
└── data/                # local scratch — raw/bronze/silver/gold (gitignored)
```

**Known issues already hit and fixed** (both were Docker Hub registry
changes, not our config being wrong):
1. `minio/mc:latest` — MinIO discontinued Docker Hub, moved to Quay →
   fixed to `quay.io/minio/mc:latest` (and `quay.io/minio/minio:latest`
   for the main image).
2. `bitnami/spark:3.5` — Bitnami moved all pinned-version tags out of
   `bitnami/` into `bitnamilegacy/` as of Sept 2025 → fixed to
   `bitnamilegacy/spark:3.5` for both spark-master and spark-worker.

**Last action taken**: edited docker-compose.yml with both fixes above,
about to re-run `docker compose up -d` and confirm `docker compose ps`
shows everything healthy/running. THIS IS WHERE YOU PICK UP — verify the
platform comes up cleanly first before moving on to anything else.

## The build plan (8 steps total — platform setup is steps 1, we're
finishing step 1)

1. Set up the local platform (docker-compose) ← **in progress, verify this first**
2. Ingest Freddie Mac data into the bronze layer (raw, untouched)
3. Clean into structured Iceberg-style tables (silver): one loan table,
   one monthly-performance table
4. Build the loan-level simulation logic in PySpark (prepay/default/pay
   hazard per loan per month) — this is the core engineering
5. Define 3 macro scenarios (baseline/adverse/severely adverse), run all
   loans through all three
6. Roll up into one portfolio reserve number per scenario, as a repeatable
   Airflow DAG
7. Prove the scale story: benchmark full-dataset runtime, then build an
   incremental-recompute path (only rerun what changed) and measure the
   speedup — this is the part that makes it a systems project, not a script
8. Package it: README with architecture + real benchmark numbers, short
   plain-English writeup for LinkedIn

## Working style notes

- Keep explanations simple and step-by-step — I want to understand what's
  happening, not just have it done for me.
- I'm learning as I go (also pursuing an Exec PG cert in GenAI/Agentic AI),
  so don't assume deep familiarity with every tool — explain briefly when
  introducing something new.
- Scoped-down v1 first (a few million loans, 3 fixed scenarios, not full
  Monte Carlo) — prove it works correctly before scaling up to the full
  55M-loan / full-scenario-sweep version.