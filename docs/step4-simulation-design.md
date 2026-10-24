# Step 4 design: the loan-level simulation

The plan, the decisions Dev approved, and the measured numbers behind them. Updated 2026-09-26.
Column meanings: [data_dictionary.md](data_dictionary.md). Silver design: [silver-design.md](silver-design.md).

## What step 4 means in plain words

For every loan that is still alive, walk forward month by month until it ends. Each month the loan can:
- **Pay** as normal,
- **Prepay** (paid off early, so the bank loses nothing), or
- **Default**, so the bank may lose money.

Each outcome has a probability, called a **hazard**. Expected loss for a month is:

`probability of default x balance at that time x share of the balance lost (LGD)`

Adding this up over all months and all loans gives the reserve. Step 5 then runs three economic scenarios by scaling the hazards up or down.

## The pieces

| # | Piece | What it produces | Status |
|---|---|---|---|
| 4a | `lake.gold.loan_current_state` | One row per alive loan at the as-of month: the starting point | **Built** (2026-09-21) |
| 4b | `lake.gold.default_hazard` | Monthly probability of moving into 90+ days late, per segment, plus the cure rate | **Built** (2026-09-26) |
| 4b3 | `lake.gold.late_transition` | From the "90+ days late" state: chance of cure, loss event, other exit, or staying late, per segment | **Built** (2026-09-26) |
| 4b4 | `lake.gold.prepay_hazard` | Monthly chance of voluntary payoff, by rate-gap bucket and age band (2022-2026 data) | **Built** (2026-09-26) |
| 4b5 | `lake.gold.reo_entry`, `lake.gold.reo_exit` | Chance of entering REO from 90+ late (by age x LTV band), and REO sale rate by months already in REO | **Built** (2026-09-26) |
| 4c | `lake.gold.lgd_by_ltv` | Loss rate (loss given default) by zero-balance code and LTV band, from real loss events | **Built** (2026-09-26); the engine will use the by-code rates |
| 4d | Simulation engine | Loan x month rows: survival, default, prepay, balance, expected loss | Not built |
| 4e | Checks | Probabilities sum to 1; a small segment recomputed by hand | Not built |

## Decisions (final, approved by Dev)

| # | Decision | Choice |
|---|---|---|
| D1 | As-of date | **2026-03**, the latest month in the data. A loan is alive if its latest row has no zero-balance code and a balance above 0. |
| D2 | What counts as default | **Refined after the pre-checks:** the hazard event is a loan's **move into 90+ days late** (`delinq_months >= 3` this month, `< 3` the month before), counted once per episode. A **cure step** is included. Loss events (zero-balance codes 02, 03, 09, 15) feed only the LGD. |
| D3 | How to build hazards | **Segment tables** (group loans, count what happened), not a regression. Simple, explainable, quick in Spark. |
| D4 | Expected value or Monte Carlo | **Expected value.** Exact and easy to verify; v1 is not Monte Carlo. |
| D5 | Modified loans and the 168 loans with no performance rows | Simulate modified loans normally with their current rate. **Exclude the 168** and report them separately. |
| D6 | Layer | **Gold**, Iceberg tables |

### Segments: 4 x 4 x 3 = 48 cells
Defined once in [`jobs/segments.py`](../jobs/segments.py) so calibration and simulation always agree.

| Band | Values |
|---|---|
| Credit score | `a: <620`, `b: 620-679`, `c: 680-739`, `d: 740+` |
| Loan-to-value | `a: <=60`, `b: 61-80`, `c: 81-90`, `d: >90` |
| Loan age (months) | `a: 0-11`, `b: 12-35`, `c: 36+` (computed per month in the simulation) |

Unknown credit score is treated as `c: 680-739`; unknown LTV as `b: 61-80`.

## Measured numbers (pre-checks, 2026-09-21)

**Alive book at 2026-03** (`precheck_simulation.py`, `build_loan_current_state.py`)
- 8,881,641 alive loans, all reported in 2026-03 (none gone quiet); total balance $2,273,718,189,631
- 6,142,909 ended loans, of which 6,081,631 prepaid (code 01)
- Simulation size: **2,499,480,409 loan-months per scenario; 7,498,441,227 cells for 3 scenarios**
- Modified alive loans: 77,908 (2,415 this month, 75,493 earlier)
- 8 alive loans have 0 remaining months (edge case to handle)

**Defaults are rare** (`precheck_simulation.py`)
- Loss events (02, 03, 09, 15): only **13,879** in 644,345,057 loan-months (0.22 per 10,000)
- Rows 90+ days late: 4,379,728 (counts rows, so one loan counts several times)
- Loss events fall from about 770 per 2016 quarter to 3 for 2025Q1: young loans have not had time to default
- Code 96 (defect) hits 37,974 loans, more than all loss events. It is a repurchase, not a loss, so it is not a default.

**The 90+ days late event** (`precheck_delinquency.py`)
- 499,596 loans ever reach 90+ days late; **456,057 (91.3%) later cure**
- 675,892 events over 624,554,726 at-risk loan-months: monthly rate **0.108%**
- That is about 49 times more events than loss events alone
- 69 cells measured, 21 thin (fewer than 30 events); almost all are the "unknown" bands, which are merged into a neighbour

| Credit band | Monthly rate of moving into 90+ days late |
|---|---|
| 740+ | 0.050% |
| 680-739 | 0.163% |
| 620-679 | 0.334% |
| under 620 | 0.428% |

Risk rises steadily as credit falls and as LTV rises, which is what mortgage risk should look like.

**Loss given default** (`precheck_simulation.py`)
- 12,402 of the 13,879 loss events have an `actual_loss`
- Overall loss rate 12.8% (slightly understated, see open items). By code: 02 = 6.2%, 03 = 11.5%, 09 = 17.6%, 15 = 15.4%

**Alive loans by band** (`loan_current_state`)
- Credit: 17,566 / 727,177 / 2,494,696 / 5,642,202 (low to high)
- LTV: 2,263,929 / 4,103,571 / 971,819 / 1,542,322
- Age: 1,079,105 / 1,588,274 / 6,214,262
- The alive book is mostly high-credit, older loans.

## Results of 4b: `lake.gold.default_hazard` (measured, 2026-09-26)

48 rows (4 x 4 x 3), no thin cell (the smallest has 59 default events).

- **Matches the pre-check exactly:** 675,892 default events over 624,554,726 months at risk = **0.1082% per month**.
  Merging "unknown" credit and LTV into the middle bands moved loans between cells without losing any.
- **Cures:** 604,985 cures over 4,308,786 months spent 90+ days late = **14.04% per month**.
- **Credit is the strongest driver.** With 61-80 LTV and 12-35 months of age: 0.047% (740+), 0.162% (680-739), 0.397% (620-679), 0.505% (under 620).
- **Higher LTV raises the rate** inside every credit band.
- **Loan age follows a seasoning curve:** lowest in the first year, highest at 12-35 months, slightly lower after 36 months in most cells
  (exception: 740+ credit, 61-80 LTV).
- **Cure rate by age:** 35-45% for loans under a year old, about 10-14% later. The cause is not tested.
- **Gap this table cannot cover:** a loan that is 90+ days late and does not cure can go to a loss event.
  That is what `late_transition` (4b3) measures. A rough check: 13,879 loss events over 4,308,786 months 90+ late is about 0.3% per month.

## Results of the second round of pre-checks (measured, 2026-09-26)

**Loans that are 90+ days late** (`late_transition`, `build_loss_and_lgd.py`, 573 s for both tables)
- 4,315,638 months started 90+ days late. Outcome of the next month: **12.68% cure, 85.62% still late, 1.35% other exit, 0.16% unknown status, 0.20% loss event** (8,586).
- Loss from this state: 0.51% per month for credit under 620, about 0.2% for the other bands.
- **20 of 48 cells are thin** (fewer than 30 loss events), almost all loans under a year old. Pool by age band x LTV band for the loss step.

**REO** (`precheck_reo_and_incentive.py`)
- Of the 13,879 loss events, the month before was: 90+ late 8,586; **RA (REO) 5,151**; under 90 days late 142. So the missing losses came through REO.
- REO sales (code 09) come from RA (5,147 of 5,153). Codes 02, 03 and 15 come from the 90+ late state.
- 6,868 loans reached RA, 6,852 of them straight from 90+ late. Time in RA: average 8.9 months, median 8, 90th percentile 17, maximum 68.
- How RA loans ended: sold (09) 5,151; still open at the end of the data 1,652; defect (96) 49; third-party sale (02) 13; payoff (01) 3.

**Loss given default** (`lgd_by_ltv`)
- Loss-event rows 13,879, with an actual loss 12,402, of which **1,253 are gains** (negative loss). Corrected overall loss rate (only rows with a loss) **14.56%** (earlier estimate 12.8% was too low).
- By code, REO sales (09) lose about 17-33% by LTV band, third-party sales (02) about 6-9%, whole-loan sales (15) about 14-18%, short sales (03) about 9-61% (few rows in some cells).
- The blended rate by LTV band is not monotone (13.3%, 17.3%, 13.1%, 12.5%), because the mix of codes differs by band.

**Prepay** (`precheck_prepay.py`, 681 s)
- 6,081,631 events over 644,345,057 loan-months = **0.944% per month**. Only 90 are payoffs at maturity.
- By rate gap (loan rate minus the market-rate proxy): 0.45% (more than 0.5 below), 0.86% (within 0.5), 2.37% (0.5-1.0 above), 3.37% (1-2 above), 3.49% (over 2 above).
- Calendar year matters inside each bucket: for 0.5-1.0 above market, 3.55% in 2020, 1.43% in 2022, 1.08% in 2024.
- Credit matters little (0.73% to 0.95%); age follows a seasoning curve (0.61%, 1.13%, 0.99%).
- The market-rate proxy per year: 3.84 (2016), 4.12, 4.56, 4.40, 3.42 (2020), 2.92 (2021), 4.50, 6.58 (2023), 6.81, 6.63, 6.08 (2026). It looks plausible; not checked against a published series.
- **The proxy is thin for April 2020 to January 2021**: only 100 to 230 loans stand behind each of those months (normal months have 170,000-200,000), because the 2020 origination files are missing.

**Alive book by rate gap at 2026-03** (proxy for today's market: 6.074%)
- More than 0.5 below market: 5,987,556 loans (67.4%, 60.6% of balance); within 0.5: 1,479,085 (16.7%); 0.5-1.0 above: 932,901 (10.5%); 1-2 above: 464,035 (5.2%); over 2 above: 18,064 (0.2%).
- So about **84% of alive loans are at or below the market rate**, which prepay slowly.

## Results of 4b4 and 4b5 (measured, 2026-09-26)

**`lake.gold.prepay_hazard`** (`build_prepay_hazard.py`, 164 s; 2022-01 to 2026-03)
- 403,554,942 loan-months, 2,225,785 prepay events = **0.5515% per month** (against 0.944% over all years: the 2020-21 wave inflated the older figure).
- The market-rate proxy is solid for these months: 50,783 to 281,450 loans behind each month. No thin cell.
- Prepay per month by rate gap (age bands 0-11 / 12-35 / 36+): more than 0.5 below market 0.33 / 0.39 / 0.50%; within 0.5: 0.47 / 0.97 / 1.12%;
  0.5-1.0 above: 1.03 / 1.81 / 1.67%; 1-2 above: 1.54 / 2.50 / 2.24%; more than 2 above: 2.36 / 2.81 / 2.78%.
- The table has 16 rows: 15 real cells plus one "unknown" row (2 loan-months with no market rate, NULL events). Harmless; the engine should ignore it.

**`lake.gold.reo_entry`** (`build_reo_transition.py`, 274 s for both REO tables)
- 6,852 entries into REO over 4,315,638 months that started 90+ late = **0.159% per month** (matches the pre-check's 6,852).
- Driven by loan-to-value: 0.04% (LTV up to 60), 0.10% (61-80), 0.18-0.20% (81-90), **0.32%** (over 90). Loans under a year old enter REO less often.
- 2 thin cells (age 0-11 with LTV up to 60 or 81-90).

**`lake.gold.reo_exit`** (by months already spent in REO, 24 = 24 or more)
- 54,411 loan-months followed a REO month: 5,147 sold, 53 other exit, 39 left REO, 49,172 still in REO. (The earlier check counted 5,151 loans ending with code 09; the difference of 4 is not explained.)
- **Monthly sale rate rises with time in REO:** 1.5% in month 1, about 6.6-9.7% in months 2-6, about 12-13.6% in months 7-12, about 13-17% afterwards and about 12.8% for 24 or more. Loans can stay long: 1,083 loan-months are at 24+ months.
- Caveat: 1,652 loans were still in REO when the data ended, so late months are censored.

## Decisions E1 to E4 (proposed by Claude, approved by Dev on 2026-09-26)

| # | Decision | Choice |
|---|---|---|
| E1 | Loss path | **Explicit REO state.** The engine has states current, 90+ late, cured, REO, sold, direct loss, prepaid. |
| E2 | Thin cells for the loss step | Pool by **age band x LTV band** |
| E3 | LGD | **By zero-balance code** (09 for REO sales, 02, 03, 15 for direct exits). This replaces the earlier idea of a single 14.56%, because with an explicit REO state the loss depends on the path. |
| E4 | Prepay calibration | **Rate-gap bucket x age band, using 2022-2026 data only** (avoids the 2020-21 refinance wave and the thin 2020 proxy, and matches an alive book that is mostly locked-in) |

## Planned before the engine (4d)

**Prepay hazard (4b4), pre-check first (`jobs/precheck_prepay.py`, read-only).** A prepay event is a row with zero-balance code 01.
Prepayment depends on the **rate incentive**: the loan's current rate minus the prevailing market rate. The data has no market rate series,
so the pre-check uses a **proxy: the balance-weighted average origination rate of loans whose first payment fell in that month**.
Known weakness of the proxy: there are no 2020 origination quarters, so report months in 2020 will have no prevailing rate
(the pre-check counts them). It also reports prepay rate by segment, by incentive bucket and by calendar year (the 2020-2021 refinance wave),
and the share of code 01 rows that are maturity payoffs (remaining months 1 or less), which are not real prepayments.

**Loss from the 90+ days late state (4b3) and LGD (4c), one script (`jobs/build_loss_and_lgd.py`).**
- `late_transition`: for every loan-month where the previous month was 90+ days late, exactly one outcome:
  loss event (codes 02, 03, 09, 15), other exit (01, 16, 96), unknown status (RA or XX without a code), cure (under 90 days), or still late.
  Outcomes are disjoint, so rates add to 1. (The cure rate here can differ slightly from `default_hazard`, which counts a cure whenever the month is under 90.)
- `lgd_by_ltv`: loss rate = sum(actual_loss) / sum(zero_bal_removal_upb), **using only rows that have an actual_loss** (fixes the understated 12.8%),
  by zero-balance code and LTV band, plus an "all codes" blend per LTV band for the engine.

**How the engine will use them (4d), state machine finalised 2026-09-26:** a loan is in one state per month: `current`, `90+ late`, `REO`, and the
end states `prepaid`, `sold` (REO sale, code 09) or `direct loss` (codes 02, 03, 15); `cured` loans return to `current`.
Each month the transition probabilities come from the gold tables, and the loss at a loss end state = balance x LGD for that code.
Expected loss = sum over loans, months and paths of probability x balance x LGD.

## Benchmark
`build_loan_current_state.py` (one scan of the 644M-row performance table plus a join): **1,205 s (about 20 minutes)** at full capacity (6 cores, 4 GB, Thrift server stopped). This is benchmark #4.

`build_default_hazard.py` (one window over all 644M rows, a join and 48 aggregates): **469 s (7.8 minutes)**, same setup. Benchmark #5.

## Open items (not yet measured or decided)
- **Remaining before the engine:** the state-machine design for 4d (how the tables combine month by month), then 4d and 4e. The prepay, REO and late-state tables are built.
- **Executor loss on long jobs:** `precheck_reo_and_incentive.py` took 30 minutes and Spark lost an executor to a heartbeat timeout (about 17 minutes in) before recovering. Cause not known; consider a larger heartbeat timeout if it recurs.
- **Loan-age effect:** mostly resolved by the 4b table (seasoning curve, see above). One cell (740+ credit, 61-80 LTV) rises after 36 months.
- **Calendar effects:** calibration mixes 2016-2026 and does not separate the COVID period. There are no 2020 origination quarters in the data (the 2020 download failed; Dev chose to leave it). This matters for step 5.
- **LGD denominator:** fixed in `lgd_by_ltv` (only rows with an actual loss are used): 14.56%.
- **DBeaver:** add the gold tables to `jobs/sql/thrift_init.sql` (they work today only by full name, e.g. `lake.gold.loan_current_state`).
- **Docs:** the gold tables are described in `data_dictionary.md`.

## Files for this step
| File | Role |
|---|---|
| `jobs/segments.py` | Segment band definitions (shared) |
| `jobs/precheck_simulation.py` | Read-only: alive loans, defaults, LGD data |
| `jobs/precheck_delinquency.py` | Read-only: 90+ days late events, cures, segment sizes |
| `jobs/build_loan_current_state.py` | Builds `lake.gold.loan_current_state` |
| `jobs/build_default_hazard.py` | Builds `lake.gold.default_hazard` (default and cure hazards, 48 segments) |
| `jobs/precheck_prepay.py` | Read-only: prepay rate by segment, rate incentive and calendar year |
| `jobs/build_loss_and_lgd.py` | Builds `lake.gold.late_transition` and `lake.gold.lgd_by_ltv` |
| `jobs/precheck_reo_and_incentive.py` | Read-only: what precedes a loss event, time in REO, alive loans by rate gap, market-rate proxy thickness |
