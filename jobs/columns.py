"""
Column names for the Freddie Mac Single-Family Loan-Level files. ONE place, used by
bronze, silver and the check scripts. Never use positional names (col_NN) anywhere.

Source of truth: docs/user_guide.pdf (Jan 2026) + verification against the real files.
Full explanation of every column: docs/data_dictionary.md

IMPORTANT: the real files differ from the guide's tables. Compared with the guide:
  * orig has 31 columns, not 32: SERVICER NAME (guide #25) is NOT in the orig file.
  * perf has 35 columns, not 32: MI CANCELLATION INDICATOR and SERVICER NAME come after the
    guide's 32 (the guide places both in the orig file), followed by one UNDOCUMENTED amount.
    Older rows end with "|" (so that 35th field is empty); from report month 202510 every row
    carries 0.00 there. (Proved by a full scan of all 37 quarters; see docs/data_dictionary.md.)

Bump LAYOUT_VERSION whenever a name or position below changes: the resumable bronze
job compares it with the manifest and re-ingests affected quarters automatically.
"""

LAYOUT_VERSION = "v3-35cols"

# --- origination file: one row per loan (31 columns, in file order) ---------------
ORIG_COLUMNS = [
    "credit_score",                    # 1   9999 = not available
    "first_pay_date",                  # 2   YYYYMM
    "first_time_buyer",                # 3   Y / N / 9
    "maturity_date",                   # 4   YYYYMM
    "msa",                             # 5   metro area code, null if none
    "mi_pct",                          # 6   0 = no MI, 999 = not available
    "num_units",                       # 7   99 = not available
    "occupancy",                       # 8   P / I / S / 9
    "cltv",                            # 9   999 = not available
    "dti",                             # 10  999 = not available
    "orig_upb",                        # 11  rounded to nearest $1,000
    "ltv",                             # 12  999 = not available
    "int_rate",                        # 13
    "channel",                         # 14  R / B / C / T / 9
    "ppm_flag",                        # 15  prepayment-penalty mortgage Y / N
    "amort_type",                      # 16  FRM (standard dataset is fixed-rate)
    "state",                           # 17
    "prop_type",                       # 18  CO / PU / MH / SF / CP / 99
    "zip3",                            # 19  first 3 digits of postal code
    "loan_seq",                        # 20  unique loan id, joins to perf
    "purpose",                         # 21  P / C / N / R / 9
    "orig_term",                       # 22  months
    "num_borrowers",                   # 23  99 = not available
    "seller",                          # 24
    "super_conforming_flag",           # 25  guide #26   (verified: values N / Y)
    "pre_relief_refi_loan_seq",        # 26  guide #27   (verified: loan-id shaped, mostly empty)
    "special_eligibility_program",     # 27  guide #28   (verified: H / F / empty)
    "relief_refi_indicator",           # 28  guide #29   (verified: Y share falls to 0 by 2023)
    "property_valuation_method",       # 29  guide #30   (verified: 7 in 2017, 1/2/4 in 2023)
    "interest_only_indicator",         # 30  guide #31   (verified: all N)
    "mi_cancellation_indicator_orig",  # 31  guide #32   constant '9999' in every row checked: unused placeholder
]

# --- performance file: one row per loan per month (35 columns, in file order) ---------
PERF_COLUMNS = [
    "loan_seq",                    # 1
    "report_period",               # 2   YYYYMM
    "current_upb",                 # 3   actual UPB (rounded to $1,000 for first 6 months)
    "delinq_status",               # 4   '00','01',... or 'RA' (REO acquisition): keep as text
    "loan_age",                    # 5   months since first payment
    "remaining_months",            # 6   months to legal maturity
    "defect_settlement_date",      # 7   YYYYMM, mostly empty
    "modification_flag",           # 8   Y / P / empty
    "zero_bal_code",               # 9   01 prepaid, 02 3rd-party sale, 03 short sale/charge-off, 09 REO, 15 whole-loan sale, 16 RPL, 96 defect
    "zero_bal_date",               # 10  YYYYMM
    "current_rate",                # 11
    "non_interest_bearing_upb",    # 12  verified: '0.00' for most rows
    "ddlpi",                       # 13  due date of last paid installment, YYYYMM (verified)
    "mi_recoveries",               # 14  loss fields 14-22: filled only on final rows
    "net_sale_proceeds",           # 15
    "non_mi_recoveries",           # 16
    "total_expenses",              # 17
    "legal_costs",                 # 18
    "maintenance_costs",           # 19
    "taxes_insurance",             # 20
    "misc_expenses",               # 21
    "actual_loss",                 # 22
    "cumulative_mod_cost",         # 23
    "rate_step_indicator",         # 24  verified: N / empty
    "payment_deferral_flag",       # 25  verified position; data has P / C (guide says Y / P)
    "eltv",                        # 26  estimated current LTV; 999 = unknown
    "zero_bal_removal_upb",        # 27
    "delinquent_accrued_interest", # 28
    "disaster_delinquency_flag",   # 29  verified: Y / empty
    "borrower_assistance_status",  # 30  verified: F / T / R / empty
    "current_month_mod_cost",      # 31
    "interest_bearing_upb",        # 32
    "mi_cancellation_indicator",   # 33  verified: 7 / N / Y  (guide puts this in orig)
    "servicer_name",               # 34  verified: company names (guide puts this in orig)
    "undocumented_amount",         # 35  in NO Freddie document. Two-decimal amount, only ever '0.00'.
                                   #     Historically on the final row of ended loans; on every row from 202510.
]

LAYOUT = {  # kind -> (column names in file order, columns to drop). Nothing is dropped: bronze keeps every field.
    "orig": (ORIG_COLUMNS, []),
    "perf": (PERF_COLUMNS, []),
}

assert len(ORIG_COLUMNS) == 31 and len(PERF_COLUMNS) == 35
assert len(set(ORIG_COLUMNS)) == 31 and len(set(PERF_COLUMNS)) == 35
