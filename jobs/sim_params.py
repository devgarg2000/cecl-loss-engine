"""
Simulation parameters: everything the engine needs, as plain Python lists.

The gold tables are tiny (12 to 48 rows each), so they are read once on the driver and turned into lists.
The engine then looks numbers up by position, so the ORDER of every list matters and is fixed here:

  default_rate[(credit * 4 + ltv) * 3 + age]   credit 0..3, ltv 0..3, age 0..2      (48 numbers)
  prepay_rate[gap * 3 + age]                    gap 0..4, age 0..2                   (15 numbers)
  late_*[age * 4 + ltv]                         rates for a loan that is 90+ days late (12 numbers each)

Meaning of the bands: see segments.py. Meaning of each rate: see docs/step4d-engine-design.md.
"""
from dataclasses import dataclass, replace

from segment_bands import AGE_BANDS, CREDIT_BANDS, INCENTIVE_BUCKETS, LTV_BANDS

MAX_MONTHS = 480                    # the longest remaining life in the data
DIRECT_LOSS_CODES = ["02", "03", "15"]
REO_SALE_CODE = "09"
MARKET_RATE_MONTHS = 3              # how many of the newest first-payment months define today's market rate


@dataclass(frozen=True)
class SimulationParameters:
    default_rate: list              # chance per month that a current loan becomes 90+ days late
    prepay_rate: list               # chance per month that a current loan is paid off early
    late_cure: list                 # a late loan gets back under 90 days
    late_direct_loss: list          # a late loan ends in a loss through codes 02, 03 or 15
    late_reo_entry: list            # a late loan becomes REO (bank owns the property)
    late_other_exit: list           # a late loan leaves without a loss (codes 01, 16, 96)
    reo_sale_probability: float     # share of finished REO episodes that end in a sale
    lgd_direct: float               # loss given default when a late loan ends through 02, 03 or 15
    lgd_reo: float                  # loss given default when a REO property is sold (code 09)
    market_rate: float              # today's market-rate proxy, in percent

    def with_zero_hazards(self):
        """Same parameters but nothing ever happens: used to test the balance arithmetic."""
        zeros = lambda values: [0.0] * len(values)
        return replace(self, default_rate=zeros(self.default_rate), prepay_rate=zeros(self.prepay_rate),
                       late_cure=zeros(self.late_cure), late_direct_loss=zeros(self.late_direct_loss),
                       late_reo_entry=zeros(self.late_reo_entry), late_other_exit=zeros(self.late_other_exit))


@dataclass(frozen=True)
class Scenario:
    """Per month (index 0 = month 1): multipliers on the default rate and on the LGD, and the market rate."""
    name: str
    default_multiplier: list
    lgd_multiplier: list
    market_rate: list


def baseline_scenario(market_rate, months=MAX_MONTHS):
    """Nothing stressed, and the market rate stays where it is today."""
    return Scenario("baseline", [1.0] * months, [1.0] * months, [float(market_rate)] * months)


def _ramped(peak, months=MAX_MONTHS, ramp_up=9, hold_to=21, fade_to=36):
    """1.0 at month 1, ramps up to peak by ramp_up, holds until hold_to, fades back to 1.0 by fade_to,
    then stays at 1.0. This is the usual regulatory stress shape (stress peaks, then the economy recovers)."""
    out = []
    for m in range(1, months + 1):
        if m <= ramp_up:
            v = 1.0 + (peak - 1.0) * (m / ramp_up)
        elif m <= hold_to:
            v = peak
        elif m <= fade_to:
            v = peak - (peak - 1.0) * ((m - hold_to) / (fade_to - hold_to))
        else:
            v = 1.0
        out.append(v)
    return out


def adverse_scenario(market_rate, months=MAX_MONTHS):
    """Default rate peaks at 1.5x, loss severity (LGD) peaks at 1.15x, both on the same ramp/fade shape."""
    return Scenario("adverse", _ramped(1.5, months), _ramped(1.15, months), [float(market_rate)] * months)


def severely_adverse_scenario(market_rate, months=MAX_MONTHS):
    """Default rate peaks at 2.5x, loss severity (LGD) peaks at 1.35x, both on the same ramp/fade shape."""
    return Scenario("severely_adverse", _ramped(2.5, months), _ramped(1.35, months), [float(market_rate)] * months)


def _rows(spark, table):
    return spark.table(table).collect()


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def load_parameters(spark, suffix="", as_of=None):
    """Read the gold tables and return SimulationParameters. Stops with a clear message if a table is incomplete.

    suffix: back-test mode. Reads <table><suffix> instead of the live tables (e.g. "_bt" -> default_hazard_bt).
    as_of: back-test mode. Market rate is computed as of this date instead of "today" (the newest data).
    """
    from pyspark.sql import functions as F          # imported here so the rest of this file works without Spark

    # ---- default hazard: 48 segments ----
    default_by_segment = {(r["credit_band"], r["ltv_band"], r["age_band"]): r["monthly_default_rate"]
                          for r in _rows(spark, "lake.gold.default_hazard" + suffix)}
    default_rate = [default_by_segment[(c, l, a)] for c in CREDIT_BANDS for l in LTV_BANDS for a in AGE_BANDS] \
        if len(default_by_segment) == 48 else None
    _require(default_rate is not None, f"default_hazard{suffix} has {len(default_by_segment)} segments, expected 48")

    # ---- prepay hazard: 15 cells (the 'unknown' row is ignored) ----
    prepay_by_cell = {(r["incentive_bucket"], r["age_band"]): r["monthly_prepay_rate"]
                      for r in _rows(spark, "lake.gold.prepay_hazard" + suffix) if r["incentive_bucket"] != "unknown"}
    _require(len(prepay_by_cell) == 15, f"prepay_hazard{suffix} has {len(prepay_by_cell)} cells, expected 15")
    prepay_rate = [prepay_by_cell[(g, a)] for g in INCENTIVE_BUCKETS for a in AGE_BANDS]

    # ---- late state: pool the 48 late_transition cells to age band x LTV band (decision E2) ----
    pooled = {}
    for r in _rows(spark, "lake.gold.late_transition" + suffix):
        cell = pooled.setdefault((r["age_band"], r["ltv_band"]), dict(months=0, cures=0, losses=0, others=0))
        cell["months"] += r["late_months"]
        cell["cures"] += r["cures"]
        cell["losses"] += r["loss_events"]
        cell["others"] += r["other_exits"]
    _require(len(pooled) == 12, f"late_transition{suffix} pooled to {len(pooled)} cells, expected 12")
    reo_entry = {(r["age_band"], r["ltv_band"]): r["monthly_reo_entry_rate"] for r in _rows(spark, "lake.gold.reo_entry" + suffix)}
    _require(len(reo_entry) == 12, f"reo_entry{suffix} has {len(reo_entry)} cells, expected 12")

    order = [(a, l) for a in AGE_BANDS for l in LTV_BANDS]
    late_cure = [pooled[k]["cures"] / pooled[k]["months"] for k in order]
    late_direct_loss = [pooled[k]["losses"] / pooled[k]["months"] for k in order]
    late_other_exit = [pooled[k]["others"] / pooled[k]["months"] for k in order]
    late_reo_entry = [reo_entry[k] for k in order]
    for i in range(12):
        leaving = late_cure[i] + late_direct_loss[i] + late_other_exit[i] + late_reo_entry[i]
        _require(leaving <= 1.0, f"late cell {order[i]}: the chances of leaving add up to {leaving:.4f}, more than 1")

    # ---- REO: how often a REO episode ends in a sale ----
    reo = spark.table("lake.gold.reo_exit" + suffix).agg(F.sum("sold").alias("sold"), F.sum("other_exit").alias("other"),
                                                  F.sum("left_reo").alias("left")).collect()[0]
    reo_sale_probability = reo["sold"] / (reo["sold"] + reo["other"] + reo["left"])

    # ---- LGD: dollar-weighted loss rate (only rows that have an actual loss), by how the loan ended ----
    by_code = {}
    for r in _rows(spark, "lake.gold.lgd_by_ltv" + suffix):
        if r["zero_bal_code"] == "all":
            continue
        cell = by_code.setdefault(r["zero_bal_code"], dict(loss=0.0, balance=0.0))
        cell["loss"] += float(r["sum_actual_loss"] or 0)
        cell["balance"] += float(r["sum_balance_of_rows_with_loss"] or 0)

    def pooled_rate(codes):
        loss = sum(by_code[c]["loss"] for c in codes)
        balance = sum(by_code[c]["balance"] for c in codes)
        return loss / balance

    # ---- market-rate proxy: balance-weighted rate of the newest loans as of "today" (or as_of, in back-test mode) ----
    loans = spark.table("lake.silver.loans")
    if as_of:
        loans_before = loans.filter(F.col("first_pay_date") < F.lit(as_of).cast("date"))
        newest = loans_before.agg(F.max("first_pay_date")).collect()[0][0]
    else:
        loans_before, newest = loans, loans.agg(F.max("first_pay_date")).collect()[0][0]
    market = (loans_before.filter(F.col("first_pay_date") >= F.add_months(F.lit(newest), -(MARKET_RATE_MONTHS - 1)))
                   .agg((F.sum(F.col("int_rate") * F.col("orig_upb")) / F.sum("orig_upb"))).collect()[0][0])

    return SimulationParameters(
        default_rate=[float(x) for x in default_rate], prepay_rate=[float(x) for x in prepay_rate],
        late_cure=late_cure, late_direct_loss=late_direct_loss, late_reo_entry=[float(x) for x in late_reo_entry],
        late_other_exit=late_other_exit, reo_sale_probability=float(reo_sale_probability),
        lgd_direct=pooled_rate(DIRECT_LOSS_CODES), lgd_reo=pooled_rate([REO_SALE_CODE]), market_rate=float(market))
