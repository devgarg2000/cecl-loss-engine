"""
The segment band labels and cut-offs, as plain Python. No Spark needed to import this file.

segments.py turns these into Spark expressions; the simulation reference and the parameter loader use the lists directly.
Bands: credit score, loan-to-value (LTV), loan age, and the rate gap (loan rate minus market rate, in percentage points).
"""
CREDIT_BANDS = ["a: <620", "b: 620-679", "c: 680-739", "d: 740+"]
LTV_BANDS = ["a: <=60", "b: 61-80", "c: 81-90", "d: >90"]
AGE_BANDS = ["a: 0-11", "b: 12-35", "c: 36+"]

# A rate gap below the first cut-off is bucket 0, between the first and second is bucket 1, and so on.
INCENTIVE_CUTOFFS = [-0.5, 0.5, 1.0, 2.0]
INCENTIVE_BUCKETS = ["a: more than 0.5 below market", "b: within 0.5 of market", "c: 0.5 to 1.0 above",
                     "d: 1.0 to 2.0 above", "e: more than 2.0 above"]
