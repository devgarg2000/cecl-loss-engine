# Step 7: the scale story — full-dataset benchmarks, then incremental recompute

What CLAUDE.md asks for here: benchmark the full-dataset runtime, then prove that an incremental
(only-rerun-what-changed) path is actually faster, with a real measured number. This is the part
that makes the project a systems build, not a script — a bank recomputes CECL every quarter when
new data and forecasts arrive; recomputing everything from scratch every time is the naive way,
recomputing only what changed is the engineering problem.

## Part 1: full-dataset runtime, consolidated

Every stage of this pipeline was benchmarked as it was built. All measured at full cluster
capacity (6 cores, 4 GB executor, Thrift server stopped, unless noted).

| Stage | Size | Time | Rate |
|---|---|---|---|
| Bronze ingest (partial, 29 quarters) | ~35 GB text, 321M loan-months | 32m40s | — (Thrift running, shared cores) |
| Bronze ingest (full, 37 quarters) | ~70 GB text, 644M loan-months | 39m16s | ~277k rows/s |
| Silver build | 644M loan-months, 72 tasks | 43m23s | ~250k rows/s |
| `loan_current_state` (gold) | scan of 644M rows + join | 20m05s | — |
| `default_hazard` (gold) | window over 644M rows + join | 7m49s | — |
| Simulation engine (sample, timed separately) | 100,000 loans, 28.0M loan-months | 88.8s | 3.17 microseconds/loan-month |
| **Full simulation, baseline scenario** (37 quarters) | 8,881,641 loans, 2.50B loan-months | **86.9 min** (5,213s) | — |
| **Full simulation, adverse scenario** | same | **79.1 min** (4,747s) | — |
| **Full simulation, severely adverse scenario** | same | **77.0 min** (4,616s) | — |

The last three rows are the real recorded totals from `lake.gold.sim_manifest` (sum of each
quarter's own measured `seconds`), run via the Airflow pipeline at 4 cores / 4 GB per task
(the DAG's resource setting — see `docs/concepts-qa.md` and `dags/cecl_pipeline_dag.py` for why
tasks run sequentially at less than full capacity: so the always-on Thrift server doesn't get
starved, and multiple driver JVMs don't compete for memory inside the Airflow container).

**Total, all three scenarios, from nothing: about 4.1 hours** (gold-layer rebuild + 3 full
scenario runs), if every table had to be rebuilt from scratch.

## Part 2: incremental recompute, measured

`build_simulation.py` is resumable per (scenario, origination quarter): each quarter's ledger
row in `sim_manifest` carries a fingerprint of the hazard tables, the scenario, the as-of month
and the engine version. A quarter is only recomputed if that fingerprint changed — otherwise the
job skips it. This models the real situation: when only *some* loans or *some* hazard inputs
change (one new quarter of data, one updated economic forecast), only the affected work should
rerun, not the whole 37-quarter, 3-scenario pipeline.

**Experiment:** force one quarter (2023Q1, baseline scenario) to recompute, under the exact same
resource conditions as the full run above (4 cores, 4 GB executor), and compare its time against
the full 37-quarter total.

```bash
spark-submit ... build_simulation.py --scenario baseline --quarters 2023Q1 --force
```

| | Time |
|---|---|
| Full rerun, all 37 quarters (baseline) | 5,213s (measured, sum of per-quarter times) |
| Incremental rerun, 1 quarter (2023Q1) | 139.6s job time / 206s wall clock (includes Spark startup) |

**Speedup:**
- **~37x** on pure compute time (5,213s / 139.6s) — the fair comparison if the full run were
  also just the sum of per-quarter compute, excluding one-time startup.
- **~25x** on real wall-clock time as a user would experience it (5,213s vs 206s) — the fairer
  comparison when both are run as a single `spark-submit` invocation, since the full run's
  Spark startup cost is paid once regardless of whether it's processing 1 quarter or 37.

Both numbers are driven by the same thing: the incremental run does about 1/37th of the
`add_simulation()` computation. This is the expected, designed-for result — the resumable
manifest pattern (also used by `ingest_bronze.py` and `build_silver.py`) exists exactly so a
quarterly CECL cycle only pays for the work that actually changed.

## What this does and doesn't prove

- It proves the **simulation layer's** incremental path works and the speedup is real and large.
- It does **not** cover bronze/silver, which are also resumable (same fingerprint/manifest
  pattern) but weren't separately re-benchmarked for a single-quarter change here — the same
  proof-of-concept would apply, with an even larger ratio since those stages scan the full
  644M-row table even for a 1-quarter answer (no per-quarter partition pruning benefit there
  beyond "skip the Iceberg write if nothing changed").
- The **gold-layer hazard tables** (`default_hazard`, `loss_and_lgd`, `prepay_hazard`,
  `reo_transition`) are **not** quarter-partitioned or fingerprinted — they always rebuild in
  full from all history (they're aggregates over the whole dataset, not per-quarter facts). A
  genuinely finer-grained incremental path for those would need incremental aggregation
  (e.g. only re-aggregating the changed quarter's contribution), which this project does not do.
  Worth noting as a limitation, not a gap in what was measured above.

## Measured 2026-10-24
