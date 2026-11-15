"""
Tool 2: explain a scenario's loss by segment. The query is CODE-authored (not written by the
LLM) so the model can only narrate numbers that actually came back from a real aggregation —
it still passes through sql_validate for defense-in-depth.
"""
import re
import time

import audit_log
import db_client
from sql_validate import validate_select

VALID_SCENARIOS = {"baseline", "adverse", "severely_adverse"}
VALID_SEGMENT_BY = {"credit_band", "ltv_band"}
QUARTER_PATTERN = re.compile(r"^20\d{2}Q[1-4]$")


def explain_scenario(scenario: str, segment_by: str = "credit_band", quarter: str | None = None) -> dict:
    if scenario not in VALID_SCENARIOS:
        return {"error": f"scenario must be one of {sorted(VALID_SCENARIOS)}, got {scenario!r}."}
    if segment_by not in VALID_SEGMENT_BY:
        return {"error": f"segment_by must be one of {sorted(VALID_SEGMENT_BY)}, got {segment_by!r}."}
    if quarter is not None and not QUARTER_PATTERN.match(quarter):
        return {"error": f"quarter must look like '2023Q1', got {quarter!r}."}

    where = f"scenario = '{scenario}'"
    if quarter:
        where += f" AND source_quarter = '{quarter}'"

    sql = f"""
        SELECT {segment_by} AS segment,
               COUNT(*) AS loan_count,
               SUM(current_upb) AS total_balance,
               SUM(expected_loss) AS total_expected_loss,
               SUM(expected_loss) / SUM(current_upb) * 100 AS loss_rate_pct
        FROM lake.gold.sim_loan_result
        WHERE {where}
        GROUP BY {segment_by}
        ORDER BY {segment_by}
    """
    validated_sql = validate_select(sql)

    started = time.time()
    try:
        result = db_client.get_db_client().run_query(validated_sql)
    except Exception as e:  # noqa: BLE001
        audit_log.log_event(
            tool="explain_scenario", scenario=scenario, quarter=quarter, segment_by=segment_by,
            validated_sql=validated_sql, status="error", error=str(e),
        )
        return {"error": f"Query failed: {e}"}

    duration_ms = round((time.time() - started) * 1000)
    columns = result["columns"]
    chart_data = [dict(zip(columns, row)) for row in result["rows"]]

    audit_log.log_event(
        tool="explain_scenario", scenario=scenario, quarter=quarter, segment_by=segment_by,
        validated_sql=validated_sql, status="ok", row_count=result["row_count"], duration_ms=duration_ms,
    )
    return {
        "columns": columns,
        "rows": result["rows"],
        "chart_data": chart_data,
        "note": "Only narrate the numbers in chart_data above — do not estimate or round beyond what is shown.",
    }
