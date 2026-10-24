"""
The simulation engine: the rules of simulation_reference.py, applied to every loan at once in Spark.

Idea: keep ONE ROW PER LOAN. The month-by-month walk runs inside that row, using Spark SQL's `aggregate`
function (a loop over the months 1..remaining_months that carries a small "state" along). So the
billions of loan-month cells are computed but never written out as rows.

The small hazard tables become constant arrays inside the expression and are looked up by position.
The order of those arrays is fixed in sim_params.py.

Read simulation_reference.py first: it is the same logic for a single loan, in plain Python.
"""
from pyspark.sql import Column, DataFrame, functions as F

from segments import CREDIT_BANDS, INCENTIVE_CUTOFFS, LTV_BANDS
from sim_params import SimulationParameters, Scenario

# what the loop carries from month to month, in this order
STATE_FIELDS = ["p_current", "p_late", "balance", "loss_direct", "loss_reo",
                "prob_prepaid", "prob_direct", "prob_reo", "prob_other"]


def _array_of(values) -> Column:
    return F.array(*[F.lit(float(v)) for v in values])


def _look_up(array_column: Column, zero_based_index: Column) -> Column:
    """element_at counts from 1, and needs a whole number."""
    return F.element_at(array_column, (zero_based_index + 1).cast("int"))


def _band_index(band_column: Column, labels) -> Column:
    """Position of a band label in its list: 0 for the first band."""
    index = F.when(band_column == labels[0], 0)
    for position, label in enumerate(labels[1:], start=1):
        index = index.when(band_column == label, position)
    return index


def _age_band_index(age_in_months: Column) -> Column:
    return F.when(age_in_months < 12, 0).when(age_in_months < 36, 1).otherwise(2)


def _rate_gap_index(gap_in_points: Column) -> Column:
    return (F.when(gap_in_points < INCENTIVE_CUTOFFS[0], 0).when(gap_in_points < INCENTIVE_CUTOFFS[1], 1)
             .when(gap_in_points < INCENTIVE_CUTOFFS[2], 2).when(gap_in_points < INCENTIVE_CUTOFFS[3], 3).otherwise(4))


def add_simulation(loans: DataFrame, params: SimulationParameters, scenario: Scenario) -> DataFrame:
    """
    loans needs: loan_seq, current_upb, current_rate, remaining_months, loan_age, delinq_status, delinq_months,
                 credit_band, ltv_band  (that is what lake.gold.loan_current_state has).
    Returns the loans with the columns: starting_state, expected_loss, prob_* and balance_end.
    """
    default_rate, prepay_rate = _array_of(params.default_rate), _array_of(params.prepay_rate)
    late_cure, late_direct = _array_of(params.late_cure), _array_of(params.late_direct_loss)
    late_reo, late_other = _array_of(params.late_reo_entry), _array_of(params.late_other_exit)
    default_multiplier, lgd_multiplier = _array_of(scenario.default_multiplier), _array_of(scenario.lgd_multiplier)
    market_rate = _array_of(scenario.market_rate)
    reo_sale, lgd_direct, lgd_reo = F.lit(params.reo_sale_probability), F.lit(params.lgd_direct), F.lit(params.lgd_reo)

    # ---- per-loan constants, worked out once ----
    prepared = (loans
                .withColumn("starting_state", F.when(F.col("delinq_status") == "RA", "R")
                                               .when(F.col("delinq_months") >= 3, "L").otherwise("C"))
                .withColumn("credit_index", _band_index(F.col("credit_band"), CREDIT_BANDS))
                .withColumn("ltv_index", _band_index(F.col("ltv_band"), LTV_BANDS))
                .withColumn("months", F.greatest(F.col("remaining_months"), F.lit(0)))
                .withColumn("balance_now", F.col("current_upb").cast("double"))
                .withColumn("rate", F.col("current_rate").cast("double"))
                .withColumn("monthly_rate", F.col("rate") / 1200))
    payment = (F.when(F.col("months") <= 0, 0.0)
                .when(F.col("monthly_rate") > 0,
                      F.col("balance_now") * F.col("monthly_rate") / (1 - F.pow(1 + F.col("monthly_rate"), -F.col("months").cast("double"))))
                .otherwise(F.col("balance_now") / F.col("months")))
    prepared = prepared.withColumn("payment", payment)

    # ---- the state at month 0 ----
    start = F.struct(
        F.when(F.col("starting_state") == "C", 1.0).otherwise(0.0).alias("p_current"),
        F.when(F.col("starting_state") == "L", 1.0).otherwise(0.0).alias("p_late"),
        F.col("balance_now").alias("balance"),
        F.lit(0.0).alias("loss_direct"), F.lit(0.0).alias("loss_reo"),
        F.lit(0.0).alias("prob_prepaid"), F.lit(0.0).alias("prob_direct"),
        F.lit(0.0).alias("prob_reo"), F.lit(0.0).alias("prob_other"))

    # ---- one month: the same steps as simulation_reference.simulate_loan ----
    def one_month(state: Column, month: Column) -> Column:
        age_index = _age_band_index(F.col("loan_age") + month)
        gap_index = _rate_gap_index(F.col("rate") - F.element_at(market_rate, month))
        late_cell = age_index * 4 + F.col("ltv_index")

        prepay = _look_up(prepay_rate, gap_index * 3 + age_index)
        default_raw = (_look_up(default_rate, (F.col("credit_index") * 4 + F.col("ltv_index")) * 3 + age_index)
                       * F.element_at(default_multiplier, month))
        default = F.least(default_raw, 1 - prepay)               # the two chances can never add up to more than 1
        cure, direct = _look_up(late_cure, late_cell), _look_up(late_direct, late_cell)
        reo, other = _look_up(late_reo, late_cell), _look_up(late_other, late_cell)
        lgd_factor = F.element_at(lgd_multiplier, month)

        p_current, p_late, balance = state["p_current"], state["p_late"], state["balance"]
        return F.struct(
            (p_current * (1 - default - prepay) + p_late * cure).alias("p_current"),
            (p_late * (1 - cure - direct - reo - other) + p_current * default).alias("p_late"),
            F.greatest(balance * (1 + F.col("monthly_rate")) - F.col("payment"), F.lit(0.0)).alias("balance"),
            (state["loss_direct"] + balance * p_late * direct * lgd_direct * lgd_factor).alias("loss_direct"),
            (state["loss_reo"] + balance * p_late * reo * reo_sale * lgd_reo * lgd_factor).alias("loss_reo"),
            (state["prob_prepaid"] + p_current * prepay).alias("prob_prepaid"),
            (state["prob_direct"] + p_late * direct).alias("prob_direct"),
            (state["prob_reo"] + p_late * reo).alias("prob_reo"),
            (state["prob_other"] + p_late * other).alias("prob_other"))

    # the months 1..n of this loan (an empty list when the loan has no months left)
    months_list = F.when(F.col("months") >= 1, F.sequence(F.lit(1), F.col("months"))).otherwise(F.array().cast("array<int>"))
    walked = prepared.withColumn("result", F.aggregate(months_list, start, one_month))

    # a loan that starts in REO is simply going to be sold
    initial_reo_loss = F.when(F.col("starting_state") == "R",
                              F.col("balance_now") * reo_sale * lgd_reo * F.element_at(lgd_multiplier, 1)).otherwise(0.0)
    return (walked
            .withColumn("expected_loss", F.col("result.loss_direct") + F.col("result.loss_reo") + initial_reo_loss)
            .select("*", *[F.col("result." + name).alias(name) for name in STATE_FIELDS if name not in ("loss_direct", "loss_reo")])
            .withColumnRenamed("balance", "balance_end")
            .drop("result", "months", "balance_now", "rate", "monthly_rate", "payment", "credit_index", "ltv_index"))
