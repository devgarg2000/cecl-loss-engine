# Silver layer design and results

Silver = the trustworthy, typed copy of the Freddie Mac data. Built from bronze on 2026-09-20.
Column meanings are in [data_dictionary.md](data_dictionary.md); the type and cleaning rule of every column is in
[`jobs/silver_schema.py`](../jobs/silver_schema.py).

## Decisions (final)

| # | Question | Decision |
|---|---|---|
| D1 | `undocumented_amount` in silver? | **No.** Stays in bronze until Freddie documents it. |
| D2 | `mi_cancellation_indicator_orig` (constant `9999`)? | **No.** |
| D3 | Derived columns in silver? | **No, silver is pure cleaning.** One exception: `delinq_months`, a numeric copy of `delinq_status` (null for `RA`, `XX`). |
| D4 | `loan_current_state` table (each loan's latest row)? | **Step 4, not silver.** It is a modelling choice (what counts as active), so it must not force a silver rebuild. |
| D5 | Dates | **`DATE`**, first day of the month |
| D6 | Quality report table | **Yes**: `lake.silver.quality_report` |
| D7 | Zero-balance code reference table | **Yes**: `lake.silver.ref_zero_balance_codes` |
| D8 | Partitioning | **`source_quarter`**; `loan_performance` is sorted by `loan_seq, report_period` |

## Tables (Iceberg, catalog `lake`, namespace `silver`)

| Table | One row is... | Rows |
|---|---|---|
| `lake.silver.loans` | one loan | 15,024,718 |
| `lake.silver.loan_performance` | one loan in one month | 644,345,057 |
| `lake.silver.quality_report` | one column in one quarter (nulls and why) | small |
| `lake.silver.ref_zero_balance_codes` | one zero-balance code | 7 |
| `lake.silver.build_manifest` | one (table, quarter) build | 74 |

## What silver changes compared with bronze

- **Types:** text becomes `int`, `date`, `decimal(14,2)`, `decimal(6,3)` or `decimal(8,3)`.
- **"Not available" becomes null:** e.g. credit score `9999`, LTV/CLTV/DTI `999`, units `99`, property valuation `7`, `eltv` `999`, `net_sale_proceeds` `U`.
  The full list is the `null_if` field in `silver_schema.py`. Whole build: 6,022,805 values in `loans` and 70,446,781 in `loan_performance`.
- **Nothing is dropped:** one silver row per bronze row. A value that cannot be converted becomes null and is counted in the quality report (0 so far).

## How it is built

`jobs/build_silver.py` (resumable, one quarter at a time): read bronze, convert, count what changed, replace that quarter in Iceberg
(atomic partition overwrite), verify silver rows equal bronze rows, write the manifest row last. A quarter is rebuilt when it is new,
failed earlier, the rules (`TRANSFORM_VERSION`) changed, or bronze was reloaded. Run it at full capacity (Thrift stopped, `--executor-memory 4g`).

## Results (measured)

- **Build:** 74 tasks (37 quarters x 2 tables), 0 failures, 0 failed conversions. Wall clock for the last 72 tasks: **43 min 23 s** at 6 cores / 4 GB
  (about 250k rows/s). Largest task: `loan_performance` 2021Q1, 233 s.
- **Proof (`jobs/verify_silver.py`, all quarters, 158 s):** 18 checks, **all PASS**: manifest counts equal bronze; `loan_seq` unique; no placeholders left;
  values inside the guide's ranges; `orig_term` equals Freddie's formula for every loan; `delinq_months` null exactly for `RA`/`XX`; no negative balances;
  every zero-balance code known; no orphan loans; no failed conversions.

## Facts to remember

- 168 loans have no performance rows (they exist in `loans` only). Step 4 must handle them.
- 27 loans have `orig_term` above 480 months; all follow the formula, kept as delivered.
- **The dataset has no 2020 origination quarters** (raw files exist for 2016-2019 and 2021-2026Q1). Loans by year of first payment show the gap.
- Reading `loan_performance` with Iceberg's Arrow vectorized reader crashed the executor JVMs, so `spark.sql.iceberg.vectorization.enabled=false`
  is set in `jobs/lake.py`, `spark_thrift.sh` and `spark_sql.sh`. Root cause not proven.

## Query it

In DBeaver (Thrift server on `localhost:10000`) or `spark_sql.sh`:
```sql
SELECT year(first_pay_date) AS yr, round(avg(credit_score),1) AS avg_score, count(*) AS loans
FROM lake.silver.loans GROUP BY 1 ORDER BY 1;
```

## Next: step 4 (the simulation)

Build `loan_current_state` (each loan's latest observed state and starting balance), decide how ended loans and the 168 loans without performance
rows are treated, then the prepay / default / pay hazards per loan per month.
