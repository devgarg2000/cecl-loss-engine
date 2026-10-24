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

## Current status (updated 2026-10-24)

Project root: `C:\Users\dev.garg\Desktop\ScalableProjects\cecl-loss-engine\cecl-loss-engine\`

- **Step 1 done:** platform is up (MinIO, Postgres, Airflow, Spark master + 1 worker with
  6 cores / 4 GB, Spark Thrift server on port 10000 for DBeaver).
- **Step 2 done:** bronze = plain Parquet in MinIO (`cecl-lake/bronze/{orig,perf}`),
  **37 quarters (2016-2019, 2021-2026Q1): 15,024,718 loans and 644,345,057 loan-months.**
  Columns follow layout `v3-35cols` (orig 31 columns, perf 35 incl. `undocumented_amount`).
  The ingest job is resumable, every quarter's row count is verified against the source text,
  and the alignment check passed on 2016Q1, 2021Q2, 2023Q1, 2017Q1 and 2025Q4.
  A full audit of the raw files found no structural problems (see docs/data_dictionary.md).
- **Step 3 done:** silver = Iceberg tables in catalog `lake` (Hadoop catalog on MinIO): `lake.silver.loans` (15,024,718),
  `lake.silver.loan_performance` (644,345,057), plus `quality_report`, `ref_zero_balance_codes`, `build_manifest`.
  Built in 43m23s at full capacity; `verify_silver.py` passed all 18 checks. Design and results: docs/silver-design.md.
  Bronze stays Parquet; silver and gold are Iceberg.
- **Known data gap (Dev decided to leave it, 2026-09-20):** there are **no 2020 origination quarters** (raw files are 2016-2019 and 2021-2026Q1)
  because the 2020 download failed. Do not ask again. To add it later: put the files in `data/raw/`, then run `ingest_bronze.py` and `build_silver.py`
  (both resumable). Keep in mind that 2020 was a low-rate, heavy-refinance period when interpreting simulation results.
- **Step 4 in progress** (full plan, decisions D1-D6 and measured numbers: `docs/step4-simulation-design.md`):
  - **4a done:** `lake.gold.loan_current_state`, 8,881,641 alive loans at 2026-03 (built in about 20 min). Simulation size is
    2.50 billion loan-months per scenario, 7.50 billion cells for 3 scenarios.
  - **Decisions:** as-of 2026-03; hazard event = a loan's move into 90+ days late, with a cure step (91.3% of such loans cure);
    loss events (codes 02, 03, 09, 15) feed only LGD; 48 segments (credit x LTV x age, defined in `jobs/segments.py`);
    expected value, not Monte Carlo; the 168 loans without performance rows are excluded and reported separately.
  - **4b done:** `lake.gold.default_hazard` (48 segments, none thin). 675,892 default events over 624,554,726 months at risk
    = 0.1082% per month; cure rate 14.04% per month. Credit is the strongest driver; loan age follows a seasoning curve.
  - **4b3 and 4c done:** `lake.gold.late_transition` (what a 90+ days late loan does next: 12.7% cure, 85.6% stay late, 0.2% loss;
    20 of 48 cells thin) and `lake.gold.lgd_by_ltv` (overall loss rate 14.56% on rows with an actual loss; REO sales lose most).
  - **Pre-checks done (2026-09-26):** REO explains the loss events not preceded by 90+ late (5,151 of 13,879 came from status RA;
    loans stay in REO about 8.9 months). Prepay is 0.944% per month and depends strongly on the rate gap (0.45% below market,
    3.4% for 1-2 points above). 84% of the alive book is at or below the market rate. The market-rate proxy is thin for Apr 2020 to Jan 2021.
  - **Decisions E1-E4 (approved):** explicit REO state; pool thin loss cells by age x LTV band; LGD by zero-balance code (not one blended rate);
    prepay by rate-gap bucket x age band using 2022-2026 data only. Details: `docs/step4-simulation-design.md`.
  - **4b4 and 4b5 done:** `lake.gold.prepay_hazard` (2022-2026: 0.5515% per month overall, from 0.33% for loans well below market to 2.8% for loans
    over 2 points above), `lake.gold.reo_entry` (0.159% per month from 90+ late; 0.32% for LTV over 90) and `lake.gold.reo_exit`
    (sale rate rises from 1.5% in month 1 to about 13-17% later). Runtimes 164 s and 274 s.
  - **4d design written** (2026-09-26): `docs/step4d-engine-design.md`, awaiting Dev's decisions F1-F8. Recommended build: a Spark SQL
    higher-order function per loan (numpy, pandas and pyarrow are NOT installed in the Spark image). F1-F8 approved 2026-09-26.
    Delinquency status `XX` = the first reporting month of a new loan (inferred from data; all 73,012 alive `XX` loans are age 0, 99.1% current
    the month after), so they start as current. Starting states: current 8,826,134; 90+ late 53,879; REO 1,628.
  - **Engine built and passing its sample test** (2026-09-26): `sim_params.py`, `simulation_reference.py` (plain Python), `simulation_spark.py`
    (one row per loan, month loop inside a Spark SQL `aggregate`), `run_simulation.py`. On 1,070 loans all checks pass: probabilities add up to 1,
    zero hazards give zero loss and a balance of 0 at maturity, and Spark equals the Python reference to about 1e-15. Per-loan totals only for now;
    segment x month totals and the debug sample come later. Parameters from the real tables: market rate 6.074%, REO sale probability 0.982,
    LGD 11.3% (direct exits) and 21.6% (REO sales).
  - **Benchmark #6 (engine speed):** 100,000 loans (28.0M loan-months) took 88.8 s on 4 cores = 3.17 microseconds per loan-month wall time
    (`run_simulation.py --timing`). Straight-line estimate for the full book: about 132 min per scenario on 4 cores, about 88 min on 6 cores
    (about 4.4 h for 3 scenarios), unmeasured. Sample lifetime expected loss: 0.0602% of balance (not yet back-tested).
  - **Write mode built and tested on one quarter** (2026-09-26): `jobs/build_simulation.py` (resumable per origination quarter, ledger `lake.gold.sim_manifest`
    with a fingerprint of hazards + scenario + as-of month + engine version; results in `lake.gold.sim_loan_result`, partitioned by scenario and quarter).
    2023Q1: 145,344 loans, expected lifetime loss $30,058,778 = 0.0723% of balance, 180.5 s = 3.95 microseconds per loan-month (4 cores, includes checks and write).
    Proof: saved table equals the ledger (rows, distinct loans, loss total), 0 empty or negative losses, one fingerprint; a rerun says "Nothing to do".
    Loss rises with LTV (0.018% to 0.148%) and falls with credit, except the tiny under-620 band (220 loans) which looks noisy (watch on the full book).
  - **Benchmark #7 (real, replaces the old 165 min estimate):** full baseline run, all 37 quarters, 8,881,641 loans,
    2.50B loan-months: 5,213s = 86.9 min (sum of real per-quarter times from `sim_manifest`, 4 cores/4GB per task).
    Adverse: 4,747s = 79.1 min. Severely adverse: 4,616s = 77.0 min. All three real, not estimated.
  - Defaults are rare (13,879 loss events in 644M loan-months), so hazards come from the 90+ days late event (675,892 events).
- **Step 4 done.**
- **Step 5 done:** 3 scenarios (baseline/adverse/severely_adverse) defined as ramp-hold-fade stress multipliers on default
  rate and LGD (see `sim_params.py`); all 8,881,641 loans run through all three via `build_simulation.py`.
- **Step 6 done:** `lake.gold.portfolio_reserve` (one row per scenario) built by `build_portfolio_reserve.py`, orchestrated as a
  repeatable Airflow DAG (`dags/cecl_pipeline_dag.py`, manual trigger, 9 tasks run sequentially — see that file's docstring for why
  not parallel). Confirmed working end-to-end with a real triggered run, not just `tasks test`. Final numbers: expected lifetime
  loss as % of balance — baseline 0.0600%, adverse 0.0686%, severely_adverse 0.0867% — all three on loan_count 8,881,641 and
  balance $2,273.72B.
- **Step 7 done:** full-dataset benchmarks consolidated and the incremental-recompute path proven. Forcing one quarter
  (2023Q1, baseline) to recompute under the same resource conditions as the full run took 139.6s job time / 206s wall clock,
  versus 5,213s for all 37 quarters — about 37x faster on pure compute, about 25x on real wall clock. Full write-up:
  `docs/step7-incremental-recompute.md`.
- **Decisions:** do not shrink the dataset unless a real problem forces it. Explore data
  with DBeaver (via the Thrift server).
- **Benchmark #1 (for step 7):** bronze load of ~35 GB text (29 quarters, 321M loan-months) took
  32m40s with a 4 to 6 core / 3 GB executor while the Thrift server was running.
- **Benchmark #2:** full bronze load of ~70 GB text (37 quarters, 644M loan-months) took 39m16s
  on 6 cores / 4 GB executor with the Thrift server stopped (about 277k rows/s). There is no 2-core baseline yet.
- **Benchmark #3:** silver build of 644M loan-months (72 tasks) took 43m23s at full capacity (about 250k rows/s).
- **Benchmark #4:** `loan_current_state` build (one scan of 644M rows plus a join) took 20m05s at full capacity.
- **Benchmark #5:** `default_hazard` build (a window over all 644M rows plus a join) took 7m49s at full capacity.
- **Git:** pushed 2026-09-26. History was rewritten and force-pushed once to remove passwords from every commit
  (MinIO, Postgres and Airflow credentials now come from `.env`). Rotating the MinIO password is still to do.
  `.env` needs `AIRFLOW_DB_PASSWORD` and `AIRFLOW_ADMIN_PASSWORD` (docker-compose reads them).
- **Disk:** C: had 15 GB free on 2026-09-26 (34 GB before silver/gold). Check `docker system df` before big jobs.

Folder structure:
```
cecl-loss-engine/
├── docker-compose.yml   # MinIO, Postgres, Airflow, Spark master/worker/Thrift
├── .env                 # MinIO credentials (gitignored, never print it)
├── docs/                # user_guide.pdf (Freddie Mac), data_dictionary.md, silver-design.md,
│                        # step4-simulation-design.md, concepts-qa.md (theory Q&A)
├── jars/                # Iceberg runtime jar (gitignored, see docker-compose.yml)
├── jobs/                # PySpark jobs + shared modules (columns.py, lake.py) + sql/
├── notebooks/           # explore_data.ipynb (pandas, small samples)
├── dags/                # Airflow DAGs (empty until step 6)
└── data/                # data/raw = source text files (gitignored); real layers live in MinIO
```

**Known issues already hit and fixed** (1 and 2 were Docker Hub registry
changes, not our config being wrong; 3 is a quirk of the Spark image):
1. `minio/mc:latest` — MinIO discontinued Docker Hub, moved to Quay →
   fixed to `quay.io/minio/mc:latest` (and `quay.io/minio/minio:latest`
   for the main image).
2. `bitnami/spark:3.5` — Bitnami moved all pinned-version tags out of
   `bitnami/` into `bitnamilegacy/` as of Sept 2025 → fixed to
   `bitnamilegacy/spark:3.5` for both spark-master and spark-worker.

3. The Spark image runs as a user with no name and no home directory, so jobs
   need `-u root` and `--conf spark.jars.ivy=/tmp/.ivy2` (see Commands).
4. Reading `lake.silver.loan_performance` with Iceberg's Arrow "vectorized" reader crashed the executor
   JVMs (SIGSEGV in libjvm, exit code 134, `hs_err_pid*.log`). Fixed by `spark.sql.iceberg.vectorization.enabled=false`
   (set in `jobs/lake.py`, `spark_thrift.sh`, `spark_sql.sh`). Root cause not proven. A JVM crash also leaves
   ~1 GB dump files per executor in `%LOCALAPPDATA%\Temp\wsl-crashes` on Windows; ask Dev before deleting them.

## Commands (from the repo root, PowerShell)

Run any Spark job (`-u root` and the ivy setting work around the image's nameless user):
```powershell
docker exec -u root cecl-spark-master spark-submit --master spark://spark-master:7077 --executor-memory 3g --conf spark.driver.host=spark-master --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark-jobs/<job>.py [args]
```
- `ingest_bronze.py [--dry-run] [--force] [--years 2017 ..] [--quarters 2023Q1 ..]`: resumable; ingests only what is new or changed.
- `check_bronze.py 2023Q1`: proves bronze columns line up with the source text.
- `build_silver.py [--dry-run] [--force] [--quarters ..] [--years ..]`: resumable, builds `lake.silver.*` from bronze (schema in `silver_schema.py`).
- `verify_silver.py [--quarters ..]`: read-only PASS/FAIL proof checks on silver.
- `profile_raw.py`, `precheck_silver.py`, `precheck_simulation.py`, `precheck_delinquency.py`: read-only audits of raw text, bronze and silver.
- `build_loan_current_state.py [--as-of 2026-03-01]`: builds `lake.gold.loan_current_state` (step 4a). `segments.py` holds the segment bands.
- `build_default_hazard.py [--min-events 30]`: builds `lake.gold.default_hazard` (step 4b).
- `build_simulation.py [--scenario baseline] [--quarters ..] [--years ..] [--dry-run] [--force]`: resumable; saves `lake.gold.sim_loan_result` one quarter at a time
  and records each in `lake.gold.sim_manifest`. Always `--dry-run` first. A change to any hazard table changes the fingerprint and makes saved quarters out of date.
- `run_simulation.py [--sample-loans 1000] [--timing --timing-loans 100000]`: tests and times the simulation engine on a sample (writes nothing). Small job: the Thrift server can stay on
  if you pass `--executor-memory 3g`. Pure-Python parts (`simulation_reference.py`, `sim_params.py`) can be tried with the `.venv` (no Spark needed).
- `precheck_prepay.py` and `precheck_reo_and_incentive.py` (read-only), and `build_loss_and_lgd.py` (builds `lake.gold.late_transition` and `lake.gold.lgd_by_ltv`): step 4b3, 4b4 and 4c.
  Long jobs can take 10-30 minutes; one lost an executor to a heartbeat timeout and recovered. Watch http://localhost:8081.
- Git Bash tip: run `docker exec` with `MSYS_NO_PATHCONV=1` or Git Bash rewrites `/opt/...` paths. PowerShell needs no such flag.
- Interactive SQL: `docker exec -it -u root cecl-spark-master bash /opt/spark-jobs/spark_sql.sh`
- DBeaver: driver "Apache Spark", host `localhost`, port `10000`, database `default`, user `root`, no password. Tables `bronze.orig`, `bronze.perf`, and views `silver.*` (from `thrift_init.sql`). Gold tables are queried by full name, e.g. `lake.gold.loan_current_state`.
- Monitor: Spark master UI http://localhost:8081, MinIO console http://localhost:9001, Airflow http://localhost:8080.
- Resource budget (Docker VM has 7.5 GB RAM): Thrift server holds 2 cores / 1 GB; batch jobs get the rest.
  Any interactive Spark tool MUST set `--total-executor-cores`, or it takes the whole cluster and starves everything else.

## Rules for working in this repo (IMPORTANT)

**The full, mandatory working-style rules are in `.claude/rules/working-style.md`. Read and follow
them on every reply.** In short (updated 2026-10-02): keep replies short; show a command with a brief
reason before running it; for anything consequential ask and wait, every time, never chain off an
earlier "go ahead"; plain commands over clever syntax; one step at a time.

1. **Show and explain every terminal command** before running it. Dev is learning and wants to see them.
2. **No positional or placeholder column names** (`col_25` etc.) in any table. Column names live only in
   `jobs/columns.py`; their meaning is documented in `docs/data_dictionary.md`. Confirm a column against
   `docs/user_guide.pdf` AND its real values before naming it, and record how sure we are.
   To rename: edit `columns.py`, bump `LAYOUT_VERSION`, re-run the bronze job.
3. **Say what was measured and what was assumed.** Do not state facts from memory as if checked
   (a wrong "32 columns" and "2 cores" were both guessed earlier).
4. **Never print secrets.** Do not read or echo `.env`, environment variables, or process command
   lines: they contain the MinIO password. Credentials reach jobs through container environment variables only.
5. **Confirm before publishing:** `git push` and anything outward-facing needs Dev's explicit go-ahead
   (see the Git section for which account).
6. **Do not shrink the data** to fix slowness unless a real problem is shown first; tune resources first.
7. **Run heavy jobs at full capacity.** For a big job (ingest, audit, silver build): announce it, stop the
   Thrift server (`docker compose stop spark-thrift`), run with `--executor-memory 4g` and no core cap,
   then `docker compose start spark-thrift` and recreate the views
   (`beeline ... -f /opt/spark-jobs/sql/thrift_init.sql`) and tell Dev DBeaver is usable again.
8. **Concept questions go in `docs/concepts-qa.md`.** When Dev asks "what is X / how does Y work", answer in
   simple words in chat and add the question and answer to that file in the same turn (rule 12 in
   `.claude/rules/working-style.md`).
9. **Check before you build:** audit new data at scale (every line, every column) before loading it,
   never only the first lines of a file. A first-lines-only check missed a real column once.

## The build plan (8 steps total)

1. Set up the local platform (docker-compose) ← **done**
2. Ingest Freddie Mac data into the bronze layer (raw, untouched; Parquet) ← **done**
3. Clean into structured Iceberg tables (silver): one loan table,
   one monthly-performance table ← **done**
4. Build the loan-level simulation logic in PySpark (prepay/default/pay
   hazard per loan per month) — this is the core engineering ← **done**
5. Define 3 macro scenarios (baseline/adverse/severely adverse), run all
   loans through all three ← **done**
6. Roll up into one portfolio reserve number per scenario, as a repeatable
   Airflow DAG ← **done**
7. Prove the scale story: benchmark full-dataset runtime, then build an
   incremental-recompute path (only rerun what changed) and measure the
   speedup — this is the part that makes it a systems project, not a script ← **done**
8. Package it: README with architecture + real benchmark numbers, short
   plain-English writeup for LinkedIn ← **next**

## Git / GitHub accounts (IMPORTANT)

This machine has TWO GitHub identities. For this project use the **personal**
one only, never the work one.

| | Account | Email | Use for this project? |
|---|---|---|---|
| Personal | `devgarg2000` | `dvgrg2000@gmail.com` | **YES — always** |
| Work | `devgarg512000-gif` | (work) | **NO — never** |

- Repo: https://github.com/devgarg2000/cecl-loss-engine (private)
- `origin` must include the username so Git Credential Manager picks the
  personal login instead of the cached work one:
  `https://devgarg2000@github.com/devgarg2000/cecl-loss-engine.git`
- Commit identity is `devgarg2000` / `dvgrg2000@gmail.com` (already set in
  global git config). Do not change it to the work email.
- Symptom of the wrong account: `remote: Repository not found` on push/fetch
  (GitHub hides private repos from other accounts). Fix by checking the remote
  URL above, not by deleting the work credential in Windows Credential Manager.
- Pushing publishes code: always confirm with Dev before `git push`.

## Working style notes

- Keep explanations simple and step-by-step — I want to understand what's
  happening, not just have it done for me.
- I'm learning as I go (also pursuing an Exec PG cert in GenAI/Agentic AI),
  so don't assume deep familiarity with every tool — explain briefly when
  introducing something new.
- Scoped-down v1 first (a few million loans, 3 fixed scenarios, not full
  Monte Carlo) — prove it works correctly before scaling up to the full
  55M-loan / full-scenario-sweep version.