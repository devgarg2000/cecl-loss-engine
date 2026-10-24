# Data Dictionary: Freddie Mac Single-Family Loan-Level Dataset

The meaning of every column in the bronze and silver tables. The names in
[`jobs/columns.py`](../jobs/columns.py) are the single source of truth for code. This file explains them.

- **Source document:** [`docs/user_guide.pdf`](user_guide.pdf) (Freddie Mac General User Guide, January 2026).
- **Dataset used:** Standard Dataset, quarterly files `orig_YYYYQn.txt` and `perf_YYYYQn.txt` (fixed-rate loans only).
- **Raw files on disk:** 37 quarters (2016 to 2019 and 2021 to 2026Q1): **15,024,718 loans** and **644,345,057 loan-months**,
  counted by a full scan. Every line has the expected field count; no Windows line endings, blank lines or non-ASCII text;
  `orig.loan_seq` is unique across all files.
- **Bronze status:** all 37 quarters are loaded under layout `v3-35cols` (74 tasks, every row count equal to the source);
  alignment verified on 2016Q1, 2021Q2 and 2025Q4. The manifest (`s3a://cecl-lake/bronze/_manifest`) is the source of truth.

## Rules for column names

1. **No positional names** (`col_25`, `col_12`, ...) in any table anyone can query.
2. To rename or reposition a column: edit `jobs/columns.py`, bump `LAYOUT_VERSION`, and re-run the bronze job.
   It detects the change from the manifest and re-ingests the affected quarters by itself.
3. A column whose meaning is not proven stays documented here as **unverified**, with the reason.

## How the tables relate

| Table | One row is... | Joins on | Where |
|---|---|---|---|
| `bronze.orig` | one loan | `loan_seq` | Parquet `s3a://cecl-lake/bronze/orig/`, all columns text |
| `bronze.perf` | one loan in one month | `loan_seq` | Parquet `s3a://cecl-lake/bronze/perf/`, all columns text |
| `lake.silver.loans` | one loan | `loan_seq` | Iceberg, typed (planned) |
| `lake.silver.loan_performance` | one loan in one month | `loan_seq` | Iceberg, typed (planned) |

Every table also carries `source_quarter` (for example `2023Q1`): the quarter of the file the row came from.
A loan's monthly rows are always in the **same quarter's** perf file as its origination row, even years later.

## Where the real files differ from the guide

The guide's column tables do not match the files we downloaded. **The files win.**

| | Guide says | Files have |
|---|---|---|
| orig columns | 32 | **31**: `SERVICER NAME` (guide #25) is absent |
| perf columns | 32 | **35**: `mi_cancellation_indicator` and `servicer_name` follow the guide's 32 (the guide puts both in orig), then `undocumented_amount` |
| `undocumented_amount` (perf #35) | not described | see "The undocumented 35th performance column" below |
| delinquency status `XX` | not described | appears only in report months 202510 and later, and **only on a loan's first month in the data** (loan age 0). Before 202510 that month was `00`. Inferred from the data (469,185 rows, all with no previous month; 99.1% are current the next month); not confirmed by Freddie. Silver leaves `delinq_months` empty for it. |
| `payment_deferral_flag` | `Y` / `P` | data holds `P` / `C` |
| `mi_cancellation_indicator_orig` | `Y` / `N` / `7` / `9` | constant `9999`: unused placeholder |

## Origination columns (31)

"Verified" says how the name was confirmed: **values** = the actual values were profiled and match the guide's definition; **guide** = position and format follow the guide, and neighbouring columns line up; **sample** = checked against real sample rows.

| # | Column | Guide field | Silver type | Missing / special values | Verified |
|---|---|---|---|---|---|
| 1 | `credit_score` | Credit Score | INT | `9999` = not available, becomes null | sample |
| 2 | `first_pay_date` | First Payment Date | DATE (YYYYMM, first of month) | | sample |
| 3 | `first_time_buyer` | First Time Homebuyer Flag | STRING | `9` = not available, becomes null | sample |
| 4 | `maturity_date` | Maturity Date | DATE | | sample |
| 5 | `msa` | Metropolitan Statistical Area | STRING | empty = none or unknown | sample |
| 6 | `mi_pct` | Mortgage Insurance % | INT | `0` = no MI, `999` becomes null | sample |
| 7 | `num_units` | Number of Units | INT | `99` becomes null | sample |
| 8 | `occupancy` | Occupancy Status | STRING (P/I/S) | `9` becomes null | sample |
| 9 | `cltv` | Original Combined LTV | INT | `999` becomes null | sample |
| 10 | `dti` | Original Debt-to-Income | INT | `999` becomes null | sample |
| 11 | `orig_upb` | Original UPB | DECIMAL(14,2) | rounded to nearest $1,000 | sample |
| 12 | `ltv` | Original LTV | INT | `999` becomes null | sample |
| 13 | `int_rate` | Original Interest Rate | DECIMAL(6,3) | | sample |
| 14 | `channel` | Channel | STRING (R/B/C/T) | `9` becomes null | sample |
| 15 | `ppm_flag` | Prepayment Penalty Flag | STRING (Y/N) | | sample |
| 16 | `amort_type` | Amortization Type | STRING | always `FRM` in the standard dataset | sample |
| 17 | `state` | Property State | STRING | | sample |
| 18 | `prop_type` | Property Type | STRING (CO/PU/MH/SF/CP) | `99` becomes null | sample |
| 19 | `zip3` | Postal Code (first 3 digits) | STRING | `00` = unknown | sample |
| 20 | `loan_seq` | Loan Sequence Number | STRING | unique per loan, format `PYYQnXXXXXXX` | sample |
| 21 | `purpose` | Loan Purpose | STRING (P/C/N/R) | `9` becomes null | sample |
| 22 | `orig_term` | Original Loan Term | INT (months) | | sample |
| 23 | `num_borrowers` | Number of Borrowers | INT | `99` becomes null; meaning of `02` differs before/after 2018Q2 (see guide) | sample |
| 24 | `seller` | Seller Name | STRING | `Other Sellers` if under 1% of the quarter's UPB | sample |
| 25 | `super_conforming_flag` | Super Conforming Flag (#26) | STRING (Y/N) | | **values** |
| 26 | `pre_relief_refi_loan_seq` | Pre-Relief Refinance Loan Sequence (#27) | STRING | empty unless relief refinance | **values** |
| 27 | `special_eligibility_program` | Special Eligibility Program (#28) | STRING (H/F/R) | `9` becomes null | **values** |
| 28 | `relief_refi_indicator` | Relief Refinance Indicator (#29) | STRING (Y/N) | | **values** |
| 29 | `property_valuation_method` | Property Valuation Method (#30) | INT (1/2/3/4) | `7` becomes null | **values** |
| 30 | `interest_only_indicator` | Interest Only Indicator (#31) | STRING (Y/N) | | **values** |
| 31 | `mi_cancellation_indicator_orig` | MI Cancellation Indicator (#32) | not carried to silver | constant `9999`, placeholder | **values** (2 quarters) |

## Performance columns (35)

| # | Column | Guide field | Silver type | Missing / special values | Verified |
|---|---|---|---|---|---|
| 1 | `loan_seq` | Loan Sequence Number | STRING | join key to orig | sample |
| 2 | `report_period` | Monthly Reporting Period | DATE (YYYYMM) | | sample |
| 3 | `current_upb` | Current Actual UPB | DECIMAL(14,2) | rounded to $1,000 for the first 6 months | sample |
| 4 | `delinq_status` | Current Loan Delinquency Status | STRING, plus derived `delinq_months` INT | `00` current, `01` = 30-59 days, ..., `RA` = REO acquisition (`delinq_months` null) | sample |
| 5 | `loan_age` | Loan Age | INT | months since first payment | sample |
| 6 | `remaining_months` | Remaining Months to Legal Maturity | INT | | sample |
| 7 | `defect_settlement_date` | Defect Settlement Date | DATE | mostly empty | **values** |
| 8 | `modification_flag` | Modification Flag | STRING (Y/P) | empty = not modified | sample |
| 9 | `zero_bal_code` | Zero Balance Code | STRING | see code list below; set on the loan's final row only | sample |
| 10 | `zero_bal_date` | Zero Balance Effective Date | DATE | | sample |
| 11 | `current_rate` | Current Interest Rate | DECIMAL(8,3) | | sample |
| 12 | `non_interest_bearing_upb` | Current Non-Interest Bearing UPB | DECIMAL(14,2) | mostly `0.00` | **values** |
| 13 | `ddlpi` | Due Date of Last Paid Installment | DATE | mostly empty | **values** |
| 14 | `mi_recoveries` | MI Recoveries | DECIMAL(14,2) | loss fields (#14 to #22): filled only on final rows | guide |
| 15 | `net_sale_proceeds` | Net Sale Proceeds | DECIMAL(14,2) | may contain `U` = unknown, becomes null | guide |
| 16 | `non_mi_recoveries` | Non MI Recoveries | DECIMAL(14,2) | | guide |
| 17 | `total_expenses` | Total Expenses | DECIMAL(14,2) | | guide |
| 18 | `legal_costs` | Legal Costs | DECIMAL(14,2) | | guide |
| 19 | `maintenance_costs` | Maintenance and Preservation Costs | DECIMAL(14,2) | | guide |
| 20 | `taxes_insurance` | Taxes and Insurance | DECIMAL(14,2) | | guide |
| 21 | `misc_expenses` | Miscellaneous Expenses | DECIMAL(14,2) | | guide |
| 22 | `actual_loss` | Actual Loss Calculation | DECIMAL(14,2) | only for zero-balance codes 02, 03, 09, 15 | guide |
| 23 | `cumulative_mod_cost` | Cumulative Modification Cost | DECIMAL(14,2) | | sample |
| 24 | `rate_step_indicator` | Interest Rate Step Indicator | STRING (Y/N) | empty = not modified | **values** |
| 25 | `payment_deferral_flag` | Payment Deferral Flag | STRING | data holds `P` / `C` (guide: `Y` / `P`), meaning of `C` unconfirmed | **values** (position) |
| 26 | `eltv` | Estimated Loan to Value | INT | `999` = unknown, becomes null | sample |
| 27 | `zero_bal_removal_upb` | Zero Balance Removal UPB | DECIMAL(14,2) | | guide |
| 28 | `delinquent_accrued_interest` | Delinquent Accrued Interest | DECIMAL(14,2) | only for codes 02, 03, 09, 15 | guide |
| 29 | `disaster_delinquency_flag` | Delinquency Due to Disaster | STRING (Y) | empty = no | **values** |
| 30 | `borrower_assistance_status` | Borrower Assistance Status Code | STRING (F/R/T) | empty = none | **values** |
| 31 | `current_month_mod_cost` | Current Month Modification Cost | DECIMAL(14,2) | | guide |
| 32 | `interest_bearing_upb` | Interest Bearing UPB | DECIMAL(14,2) | | sample |
| 33 | `mi_cancellation_indicator` | MI Cancellation Indicator | STRING (Y/N/7) | `7` = not applicable | **values** |
| 34 | `servicer_name` | Servicer Name | STRING | `Other Servicers` if under 1% of the quarter's UPB | **values** |
| 35 | `undocumented_amount` | not in the guide or `file_layout.xlsx` | DECIMAL(14,2), **not carried to silver** until understood | empty on older rows; only ever `0.00` | **values** (format only; meaning unknown) |

Columns marked **guide** hold values on only a few rows (final rows of loans that ended in a loss event),
so the neighbouring columns (#12, #13, #24, #25, #29, #30) anchor their positions. If we ever see unexpected
values there, re-check them first.

## Code lists

**Zero balance codes** (`zero_bal_code`): the reason a loan's balance reached zero.

| Code | Meaning | Kind of outcome |
|---|---|---|
| `01` | Prepaid or matured (voluntary payoff) | Prepay (or normal maturity) |
| `02` | Third party sale | Credit event, actual loss calculated |
| `03` | Short sale or charge-off | Credit event, actual loss calculated |
| `09` | REO disposition | Credit event, actual loss calculated |
| `15` | Whole loan sale | Actual loss calculated |
| `16` | Reperforming loan securitization | Sale |
| `96` | Defect prior to other termination event | Repurchase for a defect |

If several events happen in the same month, the higher-priority one is reported: 15, 16, 09, 96, 03, 02, 01.

## Value conventions in silver

- Sentinel values ("not available") become **null**; the table above lists them.
- Empty text fields are already null in bronze.
- Bronze keeps all values exactly as delivered, as text. Only silver changes types.

## Gold tables (step 4, Iceberg, catalog `lake`, namespace `gold`)

Built from silver by the jobs in `jobs/`. All use the 48 segments from `jobs/segments.py`
(credit band x loan-to-value band x loan age band). Design and measured results: [step4-simulation-design.md](step4-simulation-design.md).

| Table | One row is... | Rows | Key columns |
|---|---|---|---|
| `lake.gold.loan_current_state` | one alive loan at 2026-03 | 8,881,641 | `loan_seq`, `source_quarter`, `as_of_month`, `current_upb`, `loan_age`, `remaining_months`, `current_rate`, `delinq_status`, `delinq_months`, `modification_flag`, `eltv`, `credit_score`, `ltv`, `cltv`, `dti`, `orig_upb`, `orig_term`, `orig_rate`, `state`, `occupancy`, `purpose`, `prop_type`, `credit_band`, `ltv_band`, `age_band` |
| `lake.gold.default_hazard` | one segment | 48 | `default_exposure_months`, `default_events`, `monthly_default_rate` (move into 90+ days late); `cure_exposure_months`, `cures`, `monthly_cure_rate`; `thin`; `calibrated_at` |
| `lake.gold.late_transition` | one segment | 48 | `late_months` (months that started 90+ days late), `loss_events`, `cures`, `other_exits`, `unknown_status`, `still_late` and their rates; `thin` |
| `lake.gold.lgd_by_ltv` | one zero-balance code (or `all`) x LTV band | 20 | `loss_event_rows`, `rows_with_actual_loss`, `sum_actual_loss`, `sum_balance_of_rows_with_loss`, `rows_with_gain`, `loss_rate`, `median_loss_rate` |
| `lake.gold.prepay_hazard` | one rate-gap bucket x age band (2022 onward) | 16 (15 + one "unknown" row to ignore) | `incentive_bucket`, `age_band`, `exposure_months`, `prepay_events`, `monthly_prepay_rate`, `thin`, `calibrated_from`, `calibrated_to` |
| `lake.gold.reo_entry` | one age band x LTV band | 12 | `late_months`, `reo_entries`, `monthly_reo_entry_rate`, `thin` |
| `lake.gold.reo_exit` | one count of months already in REO (1 to 24) | 24 | `exposure_months`, `sold`, `other_exit`, `left_reo`, `still_reo`, `monthly_sale_rate`, `thin` |
| `lake.gold.sim_loan_result` | one alive loan x scenario (built quarter by quarter; only 2023Q1 so far) | 145,344 so far | `loan_seq`, `scenario`, `source_quarter`, `as_of_month`, `starting_state` (C, L, R), `credit_band`, `ltv_band`, `current_upb`, `expected_loss`, `prob_prepaid`, `prob_direct_loss`, `prob_reo`, `prob_other_exit`, `fingerprint`, `computed_at` |
| `lake.gold.sim_manifest` | one scenario x origination quarter (the build ledger) | 1 so far | `fingerprint`, `as_of_month`, `input_loans`, `input_balance`, `loans_written`, `loan_months`, `expected_loss`, `seconds`, `status` |

Definitions: **default event** = a loan's move into 90+ days late (`delinq_months` 3 or more, after under 3). **Loss event** = zero-balance code 02, 03, 09 or 15.
**REO** = status `RA` (bank owns the property after foreclosure); the REO sale is code 09. **LGD** = share of the balance lost, `sum(actual_loss) / sum(zero_bal_removal_upb)`
over rows that have an actual loss. Rows where the loss is negative are gains and are kept in the sums.

## The undocumented 35th performance column (`undocumented_amount`)

Found because the alignment check failed on 2025Q4. Neither the January 2026 user guide nor Freddie's
`file_layout.xlsx` mentions it. What we measured:

- **Format:** always a two-decimal amount, like the loss columns. The **only value ever seen is `0.00`** (full sample of all 37 quarters).
- **When it is filled:** 0% of rows for report months up to 202509, except a few hundred rows per file: the
  **final row of loans that ended** (zero-balance code present, balance `0.00`). From report month **202510 on it is filled on every row (100%)**.
- **What it does not depend on:** loan age, modification flag, servicer, or the difference between the two balance columns.
- **Same release as `XX`:** the delinquency status `XX` occurs only in report months 202510 and later, and always with this column filled.
- **Conclusion:** a new amount field that Freddie began reporting on all rows with the October 2025 reporting month. Its meaning is
  unknown, so it is kept in bronze under an honest name and is **not** used in silver until Freddie documents it.

## Open questions

- `undocumented_amount`: meaning unknown; check Freddie's newest release notes / file layout.
- Delinquency status `XX`: inferred to be the first reporting month of a new loan (see the table above); confirm in Freddie's newest release notes / file layout.
- `mi_cancellation_indicator_orig` is the constant `9999` in all 37 quarters (100% of the sampled rows): treated as an unused placeholder.
- `payment_deferral_flag = 'C'` (5,694 sampled rows next to 162,927 `P`): meaning unconfirmed.
- 4 orig loans have a `loan_seq` whose quarter differs by one from their file's quarter (for example `F20Q41275812` in the 2021Q1 file,
  first payment 202103). They are ordinary loans with a timing edge case and are kept as delivered.

## Integrity facts (measured on bronze, 2026-09-20)

- `(loan_seq, report_period)` is **unique** in every one of the 37 quarters (0 duplicates in 644,345,057 rows).
- **No orphans:** every loan in the performance data exists in the origination data.
- **27 loans have `orig_term` above 480 months** (494 to 551, maturities in 2062 to 2064) across 14 quarters. All 15,024,718 loans satisfy
  Freddie's formula `orig_term = months(maturity_date - first_pay_date) + 1` exactly, so these are consistent data, kept as delivered.
- **168 loans have no performance rows at all** (15,024,550 of 15,024,718 loans appear in perf). The simulation step must handle them.
- Loss columns #14 to #22 have well-formed dollar values, but which loss field is which is from position only.
- The last ~560 lines of the user guide (defects, modifications, actual loss, data masking) are not yet reviewed.
  Read them before the loss-given-default work in step 4.
