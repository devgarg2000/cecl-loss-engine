"""
The simulation rules for ONE loan, in plain Python. Slow and simple on purpose.

The Spark engine (simulation_spark.py) applies the same rules to millions of loans at once.
This file exists so a person can read the rules top to bottom, and so the Spark result can be compared with it.
Rules: docs/step4d-engine-design.md.

A loan is in one of these states each month, with a probability for each:
  current  under 90 days late          late  90+ days late
and it can also have left:  prepaid, ended in a direct loss (codes 02/03/15), entered REO, or another exit.
"""
from segment_bands import INCENTIVE_CUTOFFS


def age_band_index(age_in_months):
    return 0 if age_in_months < 12 else 1 if age_in_months < 36 else 2


def rate_gap_index(gap_in_points):
    """0 = well below market ... 4 = well above market."""
    return sum(gap_in_points >= cutoff for cutoff in INCENTIVE_CUTOFFS)


def scheduled_payment(balance, monthly_rate, months):
    if months <= 0:
        return 0.0
    if monthly_rate > 0:
        return balance * monthly_rate / (1 - (1 + monthly_rate) ** -months)
    return balance / months


def simulate_loan(loan, params, scenario):
    """
    loan: a dict with current_upb, current_rate, remaining_months, loan_age, credit_index, ltv_index, starting_state ('C', 'L' or 'R').
    Returns the expected loss in dollars and where the probability ended up.
    """
    balance = float(loan["current_upb"])
    monthly_rate = float(loan["current_rate"]) / 1200
    months = max(int(loan["remaining_months"]), 0)
    payment = scheduled_payment(balance, monthly_rate, months)
    start = loan["starting_state"]

    p_current = 1.0 if start == "C" else 0.0
    p_late = 1.0 if start == "L" else 0.0
    loss_direct = loss_reo = 0.0
    prob_prepaid = prob_direct = prob_reo = prob_other = 0.0

    # a loan that is already in REO is simply going to be sold: no month-by-month walk needed
    initial_reo_loss = 0.0
    if start == "R":
        initial_reo_loss = balance * params.reo_sale_probability * params.lgd_reo * scenario.lgd_multiplier[0]

    for month in range(1, months + 1):
        age_index = age_band_index(loan["loan_age"] + month)
        gap_index = rate_gap_index(float(loan["current_rate"]) - scenario.market_rate[month - 1])
        late_cell = age_index * 4 + loan["ltv_index"]

        prepay = params.prepay_rate[gap_index * 3 + age_index]
        default = params.default_rate[(loan["credit_index"] * 4 + loan["ltv_index"]) * 3 + age_index] * scenario.default_multiplier[month - 1]
        default = min(default, 1 - prepay)                       # the two chances can never add up to more than 1
        cure = params.late_cure[late_cell]
        direct = params.late_direct_loss[late_cell]
        reo = params.late_reo_entry[late_cell]
        other = params.late_other_exit[late_cell]
        lgd_multiplier = scenario.lgd_multiplier[month - 1]

        # losses happen out of the late state, on the balance at the start of the month
        loss_direct += balance * p_late * direct * params.lgd_direct * lgd_multiplier
        loss_reo += balance * p_late * reo * params.reo_sale_probability * params.lgd_reo * lgd_multiplier

        prob_prepaid += p_current * prepay
        prob_direct += p_late * direct
        prob_reo += p_late * reo
        prob_other += p_late * other

        p_current, p_late = (p_current * (1 - default - prepay) + p_late * cure,
                             p_late * (1 - cure - direct - reo - other) + p_current * default)
        balance = max(balance * (1 + monthly_rate) - payment, 0.0)

    return {"expected_loss": loss_direct + loss_reo + initial_reo_loss, "p_current": p_current, "p_late": p_late,
            "balance_end": balance, "prob_prepaid": prob_prepaid, "prob_direct": prob_direct,
            "prob_reo": prob_reo, "prob_other": prob_other}
