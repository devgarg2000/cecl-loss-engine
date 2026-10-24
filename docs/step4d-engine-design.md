# Step 4d design: the simulation engine (plan only, nothing built yet)

Written 2026-09-26. Inputs are the gold tables from step 4a to 4c (see [step4-simulation-design.md](step4-simulation-design.md)).
Terms (REO, LGD, hazard, state machine, rate gap) are explained in [concepts-qa.md](concepts-qa.md).

## Goal

For every alive loan, walk forward month by month over its remaining life and compute the **expected loss in dollars**, under a scenario.
Add it up over all loans for the portfolio reserve. It is an **expected-value** calculation (no random draws), so the answer is exact and repeatable.

Size: 8,881,641 loans, 2,499,480,409 loan-months per scenario, 7,498,441,227 cells for 3 scenarios (measured).

## The states

Each loan carries a probability of being in each state, updated once per month.

| State | Meaning | Stays or ends |
|---|---|---|
| `C` current | under 90 days late | continues |
| `L` late | 90+ days late | continues |
| prepaid | paid off early | ends, no loss |
| direct loss | loss through sale codes 02, 03, 15 | ends, loss = balance x LGD |
| REO | bank owns the property (status RA) | handled without a state, see below |
| other exit | codes 01, 16, 96 from the late state | ends, no loss |

**Starting states** at 2026-03 (measured): current 8,753,122 loans; 90+ late 53,879; REO 1,628; status `XX` 73,012 ($27.03 billion of balance).
`XX` turned out to be the first reporting month of a brand-new loan (all 73,012 are age 0; 99.1% are current the month after), so they **start as current**:
**current 8,826,134**, 90+ late 53,879, REO 1,628.

## Monthly rules (one month, loan i, month t = 1 .. remaining months)

Segment of the loan: credit band and LTV band are fixed; the age band changes as the loan ages (`age = loan_age + t`).
The rate gap changes with the scenario: `gap(t) = loan rate - market rate(t)`.

From `C`:
- to `L` with probability `h_def` = `default_hazard` for the loan's segment (times the scenario's default multiplier)
- to prepaid with probability `h_pre` = `prepay_hazard` for (rate-gap bucket, age band)
- otherwise stays `C` (check: `h_def + h_pre <= 1`)

From `L` (rates pooled by age band x LTV band because loss events are rare, decision E2):
- cure back to `C`: `late_transition` cure rate
- direct loss: `late_transition` loss rate (codes 02, 03, 15)
- enter REO: `reo_entry` rate
- other exit: overall rate from `late_transition`
- otherwise stays `L`

Balance: the scheduled balance from `current_upb`, the loan's current rate and remaining months (standard fixed-rate amortisation).
Simplification for v1: a late loan pays nothing, so its true balance is slightly higher than scheduled. The error is small because late loans are about 0.6% of the book.

**Loss in month t** (dollars):
- direct loss = `B(t)` x P(in `L` at t-1) x direct-loss rate x LGD of codes 02/03/15 x scenario LGD multiplier
- REO loss = `B(t)` x P(in `L` at t-1) x REO-entry rate x P(REO ends in a sale) x LGD of code 09 x scenario LGD multiplier

**Why REO needs no extra state:** for the lifetime reserve only the *total* matters, not when the sale happens. So a loan entering REO contributes
`balance x LGD(09) x P(sold)`, where P(sold) is about 98% of resolved REO episodes (5,147 sold against 92 other exits, from `reo_exit`).
The month of the sale (average 8.9 months later) is only needed for reports by month, and can be spread afterwards with the `reo_exit` timing. Loans that start in REO
contribute `balance x LGD(09) x P(sold)` at once.
Weakness: 1,652 REO episodes were unfinished when the data ended, so P(sold) comes from finished ones only.

**Lifetime loss of a loan** = sum over months of the losses above. The portfolio reserve is the sum over loans.

## Scenario hooks (step 5 plugs in here)

The engine takes, per scenario and per month: a default multiplier, an LGD multiplier and a market rate path.
Baseline for v1 = multipliers 1.0 and a flat market rate equal to today's proxy (6.07%). Adverse and severely adverse scale the multipliers up and the rate path.
The market rate drives prepayment only through the rate gap.

## Outputs

| Table | One row is... | Size | Purpose |
|---|---|---|---|
| `lake.gold.sim_loan_result` | one loan x scenario | about 8.9M x 3 = 26.6M | lifetime expected loss, lifetime default probability, expected prepaid share |
| `lake.gold.sim_segment_month` | one scenario x month x credit band x LTV band | small (thousands) | reserve by month and segment, the portfolio rollup for step 6 |
| `lake.gold.sim_debug_sample` | one sampled loan x month, scenario 1 | about 5,000 loans x their months (about 1.5M rows) | full paths to inspect and to test against |

**The 7.5 billion loan-month cells are computed but not stored.** Storing them would need far more disk than we have (C: had 14 GB free on 2026-09-26).
Each loan's whole path is computed in one go and only its totals are kept, plus the small debug sample.

## How to build it: options

| Option | How | Verdict |
|---|---|---|
| **A. Spark SQL higher-order function** | One row per loan; a SQL `aggregate` loop over the months runs inside the row, with the small hazard tables passed in as arrays | **Recommended.** Pure Spark, no new libraries, never creates the 2.5 billion rows, each loan is independent so it spreads over all cores. |
| B. Iterate months in DataFrame passes | A Spark job of up to 480 passes, each joining the hazards | Simple to read but 480 passes and lineage handling. Fallback if A is too awkward. |
| C. numpy/pandas per batch | `mapInPandas` with numpy | Fast to write and run, but **numpy, pandas and pyarrow are not in the Spark image** (measured). It would need a change to the Docker image and to `docker-compose.yml`, which I would explain first. |
| D. Group similar loans | Merge loans with the same segment, age, term and rate | Cuts the work but hides the scale story and complicates testing. Maybe an optimisation for step 7. |

Performance: **unmeasured.** Plan: run on a 100,000-loan sample, time it, extrapolate to 8.9M loans, then run the full book.
Expected cost is a few minutes per scenario if option A works well, but that is a guess.

## Checks (step 4e)

1. **Probabilities conserve:** for every loan and month, current + late + prepaid + lost + other exits = 1 (tolerance 1e-9).
2. **Zero-hazard test:** with all hazards set to 0, the balance falls to 0 at maturity and the loss is 0.
3. **Independent recomputation:** a separate plain-Python implementation of the same rules on about 1,000 random loans; the Spark result must match within a tiny tolerance.
4. **Segment sanity:** loss rate rises with lower credit and with higher LTV.
5. **Back-test (strongest test):** rebuild `loan_current_state` for an earlier as-of month (the script already takes `--as-of`), calibrate the hazards using only data before that month,
   run the engine, and compare predicted prepayments, defaults and losses with what really happened afterwards. This needs hazards to be rebuilt with a cut-off date, so it is extra work.

## Decisions F1 to F8 (recommended by Claude, approved by Dev on 2026-09-26)

F7 was settled on 2026-09-26 by the `XX` check (`jobs/precheck_xx_status.py`, 271 s): see the table below.

| # | Decision | Recommendation |
|---|---|---|
| F1 | Discount the losses to present value? | **No for v1** (undiscounted). Discounting is allowed but not required; add it later if needed. |
| F2 | Build option | **A** (Spark SQL higher-order function), with B as the fallback. Do not change the image unless A fails. |
| F3 | What to store | Per-loan results, segment x month totals and a 5,000-loan debug sample. **Not** the 7.5 billion cells. |
| F4 | Loan horizon | The loan's remaining months (cap 480); prepayment shortens it |
| F5 | Baseline market rate | Flat at today's proxy (6.07%) |
| F6 | Late and REO loan balances | Use the scheduled balance for late loans (small error); REO loss on the balance at entry |
| F7 | Loans with status `XX` (73,012 loans, $27B) | **Settled: start them as current.** `XX` is the first reporting month of a new loan: every `XX` row (469,185) has no previous month, all 73,012 alive ones are age 0, and 99.1% are current a month later (0.70% 1-2 months late, 0.23% prepay). |
| F8 | Back-test as part of 4e | **Yes, but after the engine works** and the four simpler checks pass |

## Open risks
- **`XX` status** is undocumented by Freddie; our reading (first month of a new loan) comes from the data, not from a document. A side effect: silver leaves
  `delinq_months` empty on those rows, so the month after a loan's first month is not counted as at risk in the hazard tables since 202510 (a very small loss of exposure).
- **Modified loans** (77,908 alive) use the same segment hazards in v1.
- **Hazards are averages over 2016-2026** (default) and 2022-2026 (prepay). They do not know about the current economy except through the scenario multipliers.
- **The market-rate proxy** is built from Freddie loans, not from a published series.
- **No 2020 origination data.**
- Loss timing by month is approximate (REO sale timing is spread after the fact).

## Code (written 2026-09-26, step 1 of the build)

| File | Role |
|---|---|
| `jobs/segment_bands.py` | Band labels and cut-offs as plain Python (no Spark needed) |
| `jobs/sim_params.py` | Reads the gold tables into plain lists in a fixed order; defines `Scenario` and the baseline |
| `jobs/simulation_reference.py` | The rules for ONE loan in plain Python (readable, and the yardstick for tests) |
| `jobs/simulation_spark.py` | The engine: one row per loan, the month loop inside a Spark SQL `aggregate` |
| `jobs/run_simulation.py` | Test run on a sample of about 1,100 loans with three checks; writes no tables |

**Deviation from the plan above:** the first version returns **per-loan totals only**. The segment x month totals (`sim_segment_month`) and the debug sample
need each loan's monthly path, so they come in a later step, once the totals are correct and timed.
The `run_simulation.py` checks cover design checks 1 (probabilities add up), 2 (zero hazards) and 3 (independent recomputation). The reference is written by the
same person from the same design, so check 3 tests that Spark matches the written rules, not that the rules themselves are right; the back-test (check 5) does that.

## Measured speed of the engine (2026-09-26, `run_simulation.py --timing`)

100,000 random alive loans (28,037,907 loan-months) on **4 cores** with a 3 GB executor, Thrift server on:
first pass 94.5 s, second pass **88.8 s** = 3.17 microseconds of wall time per loan-month (about 12.7 microseconds of core time).
Expected loss in the sample: $15,324,636 = 0.0602% of balance (the 993-loan test gave 0.056%).

Straight-line estimate for the whole book (2,499,480,409 loan-months), **not yet measured**: about 132 minutes per scenario on 4 cores,
about **88 minutes on 6 cores**, so about 4.4 hours for 3 scenarios. This is slower than the 2-10 microsecond guess made before the measurement.

Ways to make it faster (a good before/after story for step 7):
- **Piecewise-constant months.** In the baseline the hazards only change when a loan's age band changes (at month 12 and 36 of its life), so a stretch of months can be
  done with one 2x2 matrix power instead of one step per month: about 3 pieces instead of about 281 steps. Stress scenarios with monthly paths break this unless the paths are constant over a stretch.
- **Group similar loans** (option D): fewer distinct paths to compute.
- **Numpy in the Spark image** (option C): needs a change to the Docker image and `docker-compose.yml`, to be explained first.

The full run should be done **per origination quarter** (37 chunks) with a manifest, like the bronze and silver builds, so a crash or a lost executor does not throw away hours of work.
That is `jobs/build_simulation.py`, built and tested on 2023Q1 on 2026-09-26:
- **Ledger and fingerprint:** each quarter's ledger row (`lake.gold.sim_manifest`) holds a fingerprint of the hazard numbers, scenario, as-of month and engine version, plus the input size.
  A quarter is skipped only if all of them still match.
- **Result table:** `lake.gold.sim_loan_result`, partitioned by scenario and quarter; each row is one loan with its expected loss and lifetime probabilities.
- **2023Q1 result:** 145,344 loans, $30,058,778 expected loss (0.0723% of balance), 180.5 s = **3.95 microseconds per loan-month** on 4 cores (about 25% slower than the
  3.17 of the timing run, because it includes the checks and the write). Saved table equals the ledger; rerun says "Nothing to do".
- **Full-book estimate from this rate (unmeasured):** about 165 minutes per scenario on 4 cores, about 110 minutes on 6 cores.
- **Quarter sizes** (alive loans): the largest are 2021Q1 956,020 loans (246M loan-months, about 13-16 min on 4 cores), 2021Q3 853,004, 2021Q2 763,511, 2021Q4 717,017;
  the four 2021 quarters are about a third of the work.

## Link to step 7 (incremental recompute)
Every result row will carry the scenario id, the versions of the hazard tables it used, and the as-of month. When only one scenario's parameters change, only that scenario reruns.
When new quarters arrive, only loans whose inputs changed need recomputing. That is the design goal, and it is not built yet.
