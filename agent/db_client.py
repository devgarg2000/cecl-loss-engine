"""
Read-only access to the lake's gold tables, over the already-running Spark Thrift server
(the same server DBeaver already connects to — localhost:10000, no auth).

Only AGENT_DB_MODE=thrift is implemented. Confirmed working in Phase 0 (pyhive + thrift_sasl,
no C compiler needed since this server has no SASL auth). If that ever stops being true, add a
spark-submit-based fallback client here then — not before it's actually needed.
"""
from pyhive import hive

import config


class ThriftClient:
    def run_query(self, sql: str) -> dict:
        conn = hive.Connection(
            host=config.THRIFT_HOST, port=config.THRIFT_PORT, username="root", auth="NONE"
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return {
                "columns": columns,
                "rows": [list(row) for row in rows],
                "row_count": len(rows),
            }
        finally:
            conn.close()


def get_db_client():
    if config.DB_MODE != "thrift":
        raise RuntimeError(
            f"AGENT_DB_MODE={config.DB_MODE!r} is not implemented — only 'thrift' exists today."
        )
    return ThriftClient()
