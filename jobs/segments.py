"""
Segment bands, defined ONCE. The hazard calibration and the simulation both import these,
so a loan always lands in the same segment everywhere.

A loan's segment = credit band x loan-to-value (LTV) band x loan age band  (4 x 4 x 3 = 48 cells).
Agreed rules (see docs/concepts-qa.md and the step 4 decisions):
  * Unknown credit score is treated as the middle credit band (680-739).
  * Unknown LTV is treated as the most common LTV band (61-80).
  * The age band is computed per month in the simulation because a loan gets older as it runs.
"""
from pyspark.sql import Column, functions as F

# the labels and cut-offs live in segment_bands.py (plain Python); they are re-exported here for the jobs that import them
from segment_bands import AGE_BANDS, CREDIT_BANDS, INCENTIVE_BUCKETS, INCENTIVE_CUTOFFS, LTV_BANDS  # noqa: F401


def credit_band(credit_score: Column) -> Column:
    return (F.when(credit_score < 620, CREDIT_BANDS[0])
             .when(credit_score < 680, CREDIT_BANDS[1])
             .when(credit_score < 740, CREDIT_BANDS[2])
             .when(credit_score >= 740, CREDIT_BANDS[3])
             .otherwise(CREDIT_BANDS[2]))          # unknown -> middle band


def ltv_band(ltv: Column) -> Column:
    return (F.when(ltv <= 60, LTV_BANDS[0])
             .when(ltv <= 80, LTV_BANDS[1])
             .when(ltv <= 90, LTV_BANDS[2])
             .when(ltv > 90, LTV_BANDS[3])
             .otherwise(LTV_BANDS[1]))             # unknown -> most common band


def incentive_bucket(rate_incentive: Column) -> Column:
    """Rate gap in percentage points: the loan's rate minus the prevailing market rate (+ = above market)."""
    return (F.when(rate_incentive.isNull(), "unknown")
             .when(rate_incentive < INCENTIVE_CUTOFFS[0], INCENTIVE_BUCKETS[0])
             .when(rate_incentive < INCENTIVE_CUTOFFS[1], INCENTIVE_BUCKETS[1])
             .when(rate_incentive < INCENTIVE_CUTOFFS[2], INCENTIVE_BUCKETS[2])
             .when(rate_incentive < INCENTIVE_CUTOFFS[3], INCENTIVE_BUCKETS[3])
             .otherwise(INCENTIVE_BUCKETS[4]))


def age_band(loan_age: Column) -> Column:
    return (F.when(loan_age < 12, AGE_BANDS[0])
             .when(loan_age < 36, AGE_BANDS[1])
             .otherwise(AGE_BANDS[2]))
