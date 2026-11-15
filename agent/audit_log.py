"""
Append-only audit trail: one JSON line per tool call. Never writes a secret value.
"""
import datetime
import json
import os

import config


def log_event(**fields) -> None:
    fields["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
    os.makedirs(os.path.dirname(config.AUDIT_LOG_PATH), exist_ok=True)
    with open(config.AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(fields) + "\n")
