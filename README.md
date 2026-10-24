# CECL Loss Simulation Engine

A portfolio-scale simulation of bank mortgage losses under economic stress,
built on an open-source, cloud-portable data stack. Every component (MinIO,
Spark, Airflow, Postgres) runs locally via Docker — no cloud account needed
to reproduce any number below.

## The problem

US banks must report, every quarter, how much money they expect to lose on
their loan book under the CECL accounting standard (Current Expected Credit
Loss). That means simulating the entire remaining life of every loan —
month by month, until it pays off, defaults, or matures — under multiple
macroeconomic scenarios (baseline, adverse, severely adverse), then rolling
all of that up into one reserve number per scenario.

This is a genuine scale problem, not an artificially inflated one: with
~9 million active 30-year mortgages and up to 360 remaining months each,
one scenario is on the order of **2.5 billion loan-months**; three
scenarios is **7.5 billion**. And it isn't a one-time computation — it has
to be re-run every quarter when new loan data and new economic forecasts
arrive, on a reporting deadline.

## Dataset

[Freddie Mac Single-Family Loan-Level Dataset](https://freddiemac.com/research/datasets/sf-loanlevel-dataset) —
real loan-level mortgage data, free for non-commercial/academic use.
37 quarters of originations (2016-2019, 2021-2026Q1): **15.0 million loans,
644 million monthly performance records.**

## Architecture

```
Freddie Mac text files
        │
        ▼
┌───────────────┐     ┌──────────────────┐     ┌─────────────────────────────┐
│  Bronze       │ ──▶ │  Silver           │ ──▶ │  Gold                        │
│  raw Parquet  │     │  cleaned Iceberg  │     │  hazard rates, simulation    │
│  (MinIO)      │     │  tables (MinIO)   │     │  results, portfolio reserve  │
└───────────────┘     └──────────────────┘     └─────────────────────────────┘
        ▲                                                      ▲
        └──────────────────── Spark (distributed compute) ─────┘
                                       ▲
                        Airflow (orchestration, resumable DAG)
```

- **MinIO** — S3-compatible object storage; a medallion (bronze/silver/gold)
  lake, so this ports to real S3 with a config change, not a rewrite.
- **Apache Iceberg** — table format for silver/gold: schema evolution,
  time travel, and (used heavily here) atomic, resumable writes.
- **Spark** — all compute, including the simulation engine itself.
- **Airflow** — orchestrates the recurring quarterly pipeline as a DAG.

## How the simulation works

Each loan is a small state machine with four states — **current**, **90+
days late**, **REO (foreclosed, bank-owned)**, and **closed** (paid off,
prepaid, or lost) — and three competing monthly hazards: default, cure, and
prepayment. Hazard rates are measured from the loan's own history (not
assumed), segmented by credit score, loan-to-value, loan age, and the gap
between the loan's rate and the current market rate.

Rather than materializing every loan-month as a row (7.5 billion rows for
3 scenarios), the engine runs the entire month-by-month loop for a loan
**inside a single Spark SQL `aggregate()` expression, one row per loan**.
This keeps the full simulation to roughly one row per loan per scenario,
not one row per loan-month — the difference between a job that finishes
in under 90 minutes and one that doesn't practically finish at all.

Scenarios (baseline / adverse / severely adverse) are modeled as stress
multipliers on default rate and loss severity, ramping up, holding, then
fading back over the projection — the same shape macro stress scenarios
take in practice.

## Results

Expected lifetime credit loss, as a share of outstanding balance, across
the full active book (8,881,641 loans, $2,273.72B balance), as of the
engine's last full run:

| Scenario | Expected loss |
|---|---|
| Baseline | 0.0600% |
| Adverse | 0.0686% |
| Severely adverse | 0.0867% |

All three numbers come from `lake.gold.portfolio_reserve`, produced by the
same repeatable Airflow DAG (`dags/cecl_pipeline_dag.py`) end to end.

## The scale story (benchmarks, all real and measured)

| Stage | Size | Time |
|---|---|---|
| Bronze ingest | ~70 GB text → 644M loan-months | 39m16s |
| Silver build | 644M loan-months | 43m23s |
| Gold: loan_current_state | 644M rows scanned | 20m05s |
| Gold: default_hazard | 644M rows, windowed | 7m49s |
| **Full simulation, baseline** | 8.88M loans, 2.50B loan-months | **86.9 min** |
| **Full simulation, adverse** | same | **79.1 min** |
| **Full simulation, severely adverse** | same | **77.0 min** |

**Incremental recompute** — the part that makes this a systems project and
not a script: every stage is resumable, fingerprinted against its own
inputs, and skips unchanged work. Forcing a single quarter to recompute,
under identical resources to the full run, took **139.6s** versus **5,213s**
for all 37 quarters — about **37x faster** on pure compute. This is what
makes a real quarterly CECL cycle practical: only the data and scenarios
that actually changed get recomputed, not the whole 15-million-loan book.
Full write-up and methodology: [`docs/step7-incremental-recompute.md`](docs/step7-incremental-recompute.md).

## Running it yourself

```
docker compose up -d
```

Brings up MinIO, Postgres, Airflow, and a 1-master/1-worker Spark cluster
(6 cores / 6 GB). From there:

1. `jobs/ingest_bronze.py` — load raw Freddie Mac text into bronze (resumable).
2. `jobs/build_silver.py` — clean bronze into Iceberg silver tables (resumable).
3. Trigger the `cecl_pipeline` DAG in the Airflow UI (http://localhost:8080) —
   builds all gold hazard tables, runs all three scenarios, and rolls up the
   portfolio reserve, end to end.

Full command reference, design decisions, and every number behind this
README: `CLAUDE.md` and `docs/`.

## What this project is (and isn't)

This is an infrastructure/systems build, not a prediction model — the point
is proving a pipeline can correctly and efficiently simulate billions of
loan-months at scale and recompute only what changed, the way a real bank's
quarterly reporting cycle demands. It deliberately isn't a Kaggle-style
"predict default" notebook.
