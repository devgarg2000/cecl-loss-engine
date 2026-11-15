"""Tool 1: a read-only, schema-grounded SQL question against the gold tables."""
import time

import audit_log
import db_client
from sql_validate import SqlValidationError, validate_select


def sql_query(sql: str) -> dict:
    try:
        validated_sql = validate_select(sql)
    except SqlValidationError as e:
        audit_log.log_event(tool="sql_query", requested_sql=sql, validation=f"rejected: {e}", status="error")
        return {"error": str(e)}

    started = time.time()
    try:
        result = db_client.get_db_client().run_query(validated_sql)
    except Exception as e:  # noqa: BLE001 - surfaced to the LLM as a tool result, not a crash
        audit_log.log_event(
            tool="sql_query", requested_sql=sql, validated_sql=validated_sql,
            validation="passed", status="error", error=str(e),
        )
        return {"error": f"Query failed: {e}"}

    duration_ms = round((time.time() - started) * 1000)
    audit_log.log_event(
        tool="sql_query", requested_sql=sql, validated_sql=validated_sql,
        validation="passed", status="ok", row_count=result["row_count"], duration_ms=duration_ms,
    )
    return result
