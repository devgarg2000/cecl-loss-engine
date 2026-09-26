"""
Silver schema: the TYPE and CLEANING RULE of every silver column, in one place.

Names come from columns.py (never invent a name here). Meaning of every column:
docs/data_dictionary.md. The build job (build_silver.py) reads this file to create the
Iceberg tables and to convert bronze text into typed values.

Rules
  * Nothing is dropped: one silver row per bronze row.
  * "null_if" lists placeholder values that mean "not available"; they become null.
  * A value that cannot be converted also becomes null and is counted in the quality report.
  * Deliberately NOT carried into silver (decisions D1, D2):
      perf.undocumented_amount          (meaning unknown, stays in bronze)
      orig.mi_cancellation_indicator_orig (constant '9999', unused placeholder)

Bump TRANSFORM_VERSION whenever a rule here changes: the build job then rebuilds the affected quarters.
"""
from dataclasses import dataclass

TRANSFORM_VERSION = "s1"


@dataclass(frozen=True)
class Col:
    name: str
    typ: str = "string"        # Spark / Iceberg type
    kind: str = "text"         # text | int | dec | date | delinq_months (derived)
    null_if: tuple = ()        # placeholder values that become null
    comment: str = ""
    from_bronze: bool = True   # False for derived columns


def _t(name, comment="", null_if=()):
    return Col(name, "string", "text", tuple(null_if), comment)


def _i(name, comment="", null_if=()):
    return Col(name, "int", "int", tuple(null_if), comment)


def _d(name, comment=""):
    return Col(name, "date", "date", (), comment)


def _m(name, comment="", p=14, s=2, null_if=()):
    return Col(name, f"decimal({p},{s})", "dec", tuple(null_if), comment)


# ---------------------------------------------------------------- loans (one row per loan)
LOANS = [
    _i("credit_score", "Credit score 300-850. Null if not available (9999)", ("9999",)),
    _d("first_pay_date", "First payment due date (first day of that month)"),
    _t("first_time_buyer", "First-time homebuyer Y/N. Null if not available (9)", ("9",)),
    _d("maturity_date", "Scheduled final payment month (first day of that month)"),
    _t("msa", "Metro area code. Null if not in a metro area or unknown"),
    _i("mi_pct", "Mortgage insurance percent. 0 means no MI. Null if not available (999)", ("999",)),
    _i("num_units", "Number of units 1-4. Null if not available (99)", ("99",)),
    _t("occupancy", "P primary, I investment, S second home. Null if not available (9)", ("9",)),
    _i("cltv", "Original combined loan-to-value percent. Null if not available (999)", ("999",)),
    _i("dti", "Original debt-to-income percent. Null if not available (999)", ("999",)),
    _m("orig_upb", "Original unpaid principal balance, rounded to nearest 1000 dollars"),
    _i("ltv", "Original loan-to-value percent. Null if not available (999)", ("999",)),
    _m("int_rate", "Original interest rate percent", 6, 3),
    _t("channel", "R retail, B broker, C correspondent, T third party not specified. Null if not available (9)", ("9",)),
    _t("ppm_flag", "Prepayment penalty mortgage Y/N"),
    _t("amort_type", "FRM fixed rate mortgage (standard dataset is fixed rate only)"),
    _t("state", "Property state, two letters"),
    _t("prop_type", "CO condo, PU planned unit, MH manufactured, SF single family, CP co-op. Null if not available (99)", ("99",)),
    _t("zip3", "First three digits of the postal code"),
    _t("loan_seq", "Unique loan id. Joins to loan_performance"),
    _t("purpose", "P purchase, C cash-out refi, N no-cash-out refi, R refi not specified. Null if not available (9)", ("9",)),
    _i("orig_term", "Original loan term in months"),
    _i("num_borrowers", "Number of borrowers. Null if not available (99)", ("99",)),
    _t("seller", "Seller name, or OTHER for sellers under 1 percent of the quarter"),
    _t("super_conforming_flag", "Super conforming loan Y/N"),
    _t("pre_relief_refi_loan_seq", "Loan id of the loan this relief refinance replaced. Empty otherwise"),
    _t("special_eligibility_program", "H Home Possible, F HFA Advantage, R Refi Possible. Null if none or not available"),
    _t("relief_refi_indicator", "Relief refinance loan Y/N"),
    _i("property_valuation_method", "1 appraisal waiver, 2 appraisal, 3 other, 4 ACE+PDR. Null if not available (7)", ("7",)),
    _t("interest_only_indicator", "Interest-only loan Y/N"),
]

# ------------------------------------------------- loan_performance (one row per loan per month)
PERF = [
    _t("loan_seq", "Loan id. Joins to loans"),
    _d("report_period", "Reporting month (first day of that month)"),
    _m("current_upb", "Actual unpaid principal balance at month end"),
    _t("delinq_status", "As delivered: 00 current, 01 = 30-59 days late, ..., RA REO acquisition, XX unknown (new from 202510)"),
    Col("delinq_months", "int", "delinq_months", (), "Months delinquent as a number. Null when delinq_status is RA or XX", from_bronze=False),
    _i("loan_age", "Months since first payment"),
    _i("remaining_months", "Months to legal maturity"),
    _d("defect_settlement_date", "Defect settlement month, mostly empty"),
    _t("modification_flag", "Y modified this month, P modified earlier. Null if not modified"),
    _t("zero_bal_code", "Why the balance reached zero, on the loan's final row only. See ref_zero_balance_codes"),
    _d("zero_bal_date", "Month the zero balance event happened"),
    _m("current_rate", "Current interest rate percent", 8, 3),
    _m("non_interest_bearing_upb", "Non-interest-bearing part of the balance"),
    _d("ddlpi", "Due date of last paid installment"),
    _m("mi_recoveries", "Mortgage insurance recoveries (loss field)"),
    _m("net_sale_proceeds", "Net sale proceeds (loss field). Null if unknown (U)", null_if=("U",)),
    _m("non_mi_recoveries", "Non-MI recoveries (loss field)"),
    _m("total_expenses", "Total expenses (loss field)"),
    _m("legal_costs", "Legal costs (loss field)"),
    _m("maintenance_costs", "Maintenance and preservation costs (loss field)"),
    _m("taxes_insurance", "Taxes and insurance (loss field)"),
    _m("misc_expenses", "Miscellaneous expenses (loss field)"),
    _m("actual_loss", "Actual loss, only for zero balance codes 02, 03, 09, 15"),
    _m("cumulative_mod_cost", "Cumulative modification cost"),
    _t("rate_step_indicator", "Step-rate modification Y/N. Null if not modified"),
    _t("payment_deferral_flag", "Payment deferral: P earlier, C in data (meaning of C unconfirmed)"),
    _i("eltv", "Estimated current loan-to-value percent. Null if unknown (999)", ("999",)),
    _m("zero_bal_removal_upb", "Balance removed at the zero balance event"),
    _m("delinquent_accrued_interest", "Delinquent accrued interest at the zero balance event"),
    _t("disaster_delinquency_flag", "Delinquent due to disaster: Y. Null otherwise"),
    _t("borrower_assistance_status", "F forbearance, R repayment plan, T trial period. Null if none"),
    _m("current_month_mod_cost", "Modification cost this month"),
    _m("interest_bearing_upb", "Interest-bearing part of the balance"),
    _t("mi_cancellation_indicator", "MI cancellation: Y cancelled, N not, 7 not applicable"),
    _t("servicer_name", "Servicer name, or OTHER for servicers under 1 percent of the quarter"),
]

# tables: short name -> (bronze kind, iceberg table, column spec, write order)
TABLES = {
    "loans": ("orig", "lake.silver.loans", LOANS, "loan_seq"),
    "loan_performance": ("perf", "lake.silver.loan_performance", PERF, "loan_seq, report_period"),
}

# columns deliberately not carried into silver (kept in bronze)
NOT_CARRIED = {
    "orig": {"mi_cancellation_indicator_orig"},
    "perf": {"undocumented_amount"},
}

# reference table: why a loan's balance reached zero (from the Freddie Mac user guide)
ZERO_BAL_CODES = [
    # code, description, actual_loss_calculated, priority (1 = highest when several events coincide)
    ("15", "Whole loan sale", True, 1),
    ("16", "Reperforming loan securitization", False, 2),
    ("09", "REO disposition", True, 3),
    ("96", "Defect prior to other termination event", False, 4),
    ("03", "Short sale or charge-off", True, 5),
    ("02", "Third party sale", True, 6),
    ("01", "Prepaid or matured (voluntary payoff)", False, 7),
]


def expr(c):
    """Spark expression that turns the bronze text column `c.name` into the typed silver value."""
    from pyspark.sql import functions as F

    if c.kind == "delinq_months":
        s = F.col("delinq_status")
        return F.when(s.rlike(r"^\d+$"), s.cast("int"))
    raw = F.col(c.name)
    cleaned = F.when(raw.isin(*c.null_if), F.lit(None).cast("string")).otherwise(raw) if c.null_if else raw
    if c.kind == "text":
        return cleaned
    if c.kind == "date":
        return F.to_date(F.concat(cleaned, F.lit("01")), "yyyyMMdd")
    return cleaned.cast(c.typ)          # int | dec


def ddl_columns(spec):
    """'name type COMMENT ...' lines for CREATE TABLE, plus the partition column."""
    def q(s):
        return s.replace("'", "")
    lines = [f"{c.name} {c.typ} COMMENT '{q(c.comment)}'" for c in spec]
    lines.append("source_quarter string COMMENT 'Quarter of the Freddie Mac file this row came from'")
    return ",\n  ".join(lines)


def check_against_columns():
    """Every silver column must exist in columns.py; everything else must be listed as not carried."""
    from columns import ORIG_COLUMNS, PERF_COLUMNS

    for short, (kind, _table, spec, _order), all_cols in (
            ("loans", TABLES["loans"], ORIG_COLUMNS), ("loan_performance", TABLES["loan_performance"], PERF_COLUMNS)):
        bronze_names = {c.name for c in spec if c.from_bronze}
        unknown = bronze_names - set(all_cols)
        missing = set(all_cols) - bronze_names - NOT_CARRIED[kind]
        assert not unknown, f"{short}: not in columns.py: {sorted(unknown)}"
        assert not missing, f"{short}: bronze columns neither typed nor listed as not carried: {sorted(missing)}"
        assert len({c.name for c in spec}) == len(spec), f"{short}: duplicate column names"
    return True


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    check_against_columns()
    print("silver_schema OK:", {k: len(v[2]) for k, v in TABLES.items()}, "silver columns (before source_quarter)")
