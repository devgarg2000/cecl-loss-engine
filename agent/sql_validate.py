"""
Structural SELECT-only guardrail for the sql_query tool. This is a trusted-single-operator,
local-demo validator, not a hardened multi-tenant SQL sandbox — good enough because this project
is never hosted for other people, but documented here so that limit is explicit, not assumed.

Rejects anything that isn't exactly one read-only SELECT (or WITH ... SELECT) against an
allowlisted lake.gold table. Raises SqlValidationError with a plain-English reason on rejection;
callers should return that reason to the LLM as a tool result (so it can retry), not crash.
"""
import re

import sqlparse

from schema_context import GOLD_TABLES

BLOCKED_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "MERGE",
    "GRANT", "REVOKE", "CALL", "COPY", "LOAD", "SET", "USE", "VACUUM", "OPTIMIZE",
]
DEFAULT_LIMIT = 500


class SqlValidationError(Exception):
    pass


def _extract_cte_aliases(sql_upper: str) -> set:
    return {m.lower() for m in re.findall(r"\b(\w+)\s+AS\s*\(", sql_upper)}


def _extract_referenced_tables(sql_upper: str) -> set:
    return {m.lower() for m in re.findall(r"(?:FROM|JOIN)\s+([A-Za-z0-9_.]+)", sql_upper)}


def validate_select(sql: str) -> str:
    """Returns the (possibly LIMIT-appended) query if it passes, else raises SqlValidationError."""
    cleaned = sqlparse.format(sql, strip_comments=True).strip().rstrip(";").strip()
    if not cleaned:
        raise SqlValidationError("Empty query.")

    statements = [s for s in sqlparse.parse(cleaned) if s.token_first(skip_cm=True) is not None]
    if len(statements) != 1:
        raise SqlValidationError("Only exactly one SQL statement is allowed per call.")

    sql_upper = cleaned.upper()
    is_select = sql_upper.startswith("SELECT")
    is_cte_select = sql_upper.startswith("WITH")
    if not (is_select or is_cte_select):
        raise SqlValidationError("Only SELECT (optionally with a WITH clause) statements are allowed.")

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql_upper):
            raise SqlValidationError(f"The keyword '{keyword}' is not allowed in this read-only tool.")

    cte_aliases = _extract_cte_aliases(sql_upper)
    referenced_tables = _extract_referenced_tables(sql_upper)
    unknown_tables = {
        t for t in referenced_tables
        if t.lower() not in GOLD_TABLES and t.lower() not in cte_aliases
    }
    if unknown_tables:
        raise SqlValidationError(
            f"These tables are not in the allowed gold-table list: {sorted(unknown_tables)}. "
            f"Allowed tables: {sorted(GOLD_TABLES)}."
        )

    if not re.search(r"\bLIMIT\b", sql_upper):
        cleaned = f"{cleaned} LIMIT {DEFAULT_LIMIT}"

    return cleaned
