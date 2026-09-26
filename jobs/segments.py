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

CREDIT_BANDS = ["a: <620", "b: 620-679", "c: 680-739", "d: 740+"]
LTV_BANDS = ["a: <=60", "b: 61-80", "c: 81-90", "d: >90"]
AGE_BANDS = ["a: 0-11", "b: 12-35", "c: 36+"]


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


def age_band(loan_age: Column) -> Column:
    return (F.when(loan_age < 12, AGE_BANDS[0])
             .when(loan_age < 36, AGE_BANDS[1])
             .otherwise(AGE_BANDS[2]))
