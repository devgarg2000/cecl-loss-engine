"""
Tool 3: the agentic-action tool. `run_scenario_simulation` is the ONLY function the LLM can
call (it is the only one listed in tool_specs.DISPATCH) and it NEVER runs a real Spark job —
it only runs the existing job's own --dry-run, locks in the exact real command, and stores it
under a pending_action_id. The real job only runs when a human explicitly confirms, by calling
`_execute_confirmed_run` directly (from the CLI's y/N prompt, or later a /api/confirm endpoint)
— that function is never registered as a tool, so the model has no path to reach it.

All 37 quarters x 3 scenarios are already fully built (confirmed live in Phase 0), so a normal
(force=False) run will usually find its quarter already up to date and do nothing — that's the
resumable design working as intended, not a bug. Pass force=True to make it recompute anyway.
"""
import re
import subprocess
import time
import uuid

import audit_log
import config
import db_client

VALID_SCENARIOS = {"baseline", "adverse", "severely_adverse"}
QUARTER_PATTERN = re.compile(r"^20\d{2}Q[1-4]$")

PENDING_ACTIONS: dict[str, dict] = {}


def _clean_output(text: str, max_chars: int = 4000) -> str:
    """Spark's log4j lines (timestamp + INFO/WARN) drown out build_simulation.py's own print()
    output. Drop them so the actual plan/result summary isn't lost to a tail-truncation."""
    lines = [line for line in text.splitlines() if " INFO " not in line and " WARN " not in line]
    return "\n".join(lines)[-max_chars:]


def _validate_quarters(quarters: list[str] | None) -> list[str]:
    quarters = quarters or config.DEFAULT_RUN_QUARTERS
    for q in quarters:
        if not QUARTER_PATTERN.match(q):
            raise ValueError(f"quarter must look like '2023Q1', got {q!r}.")
    return quarters[: config.MAX_QUARTERS_PER_RUN]


def _build_command(scenario: str, quarters: list[str], force: bool, dry_run: bool) -> list[str]:
    cmd = config.SPARK_SUBMIT_BASE + [
        "/opt/spark-jobs/build_simulation.py",
        "--scenario", scenario,
        "--quarters", *quarters,
    ]
    if force:
        cmd.append("--force")
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def run_scenario_simulation(scenario: str, quarters: list[str] | None = None, force: bool = False) -> dict:
    if scenario not in VALID_SCENARIOS:
        return {"error": f"scenario must be one of {sorted(VALID_SCENARIOS)}, got {scenario!r}."}
    try:
        quarters = _validate_quarters(quarters)
    except ValueError as e:
        return {"error": str(e)}

    dry_run_cmd = _build_command(scenario, quarters, force, dry_run=True)
    real_cmd = _build_command(scenario, quarters, force, dry_run=False)

    proc = subprocess.run(dry_run_cmd, capture_output=True, text=True, timeout=120)

    pending_action_id = uuid.uuid4().hex[:8]
    PENDING_ACTIONS[pending_action_id] = {
        "command": real_cmd, "scenario": scenario, "quarters": quarters, "force": force,
    }

    audit_log.log_event(
        tool="run_scenario_simulation", scenario=scenario, quarters=quarters, force=force,
        dry_run_command=" ".join(dry_run_cmd), dry_run_exit_code=proc.returncode,
        pending_action_id=pending_action_id, status="proposed",
    )
    return {
        "pending_action_id": pending_action_id,
        "dry_run_output": _clean_output(proc.stdout + proc.stderr),
        "command_preview": " ".join(real_cmd),
        "message": (
            "This is a PROPOSAL only — nothing has run for real. Tell the human what this would do "
            "and ask them to explicitly confirm before anything real happens."
        ),
    }


def _execute_confirmed_run(pending_action_id: str) -> dict:
    """Not a tool. Only reachable from an explicit human confirmation (CLI y/N, or /api/confirm)."""
    action = PENDING_ACTIONS.pop(pending_action_id, None)
    if action is None:
        return {"error": "Unknown or already-used pending_action_id."}

    started = time.time()
    proc = subprocess.run(action["command"], capture_output=True, text=True, timeout=7200)
    duration_s = round(time.time() - started, 1)

    quarters_list = ", ".join(f"'{q}'" for q in action["quarters"])
    manifest_sql = (
        f"SELECT scenario, source_quarter, loans_written, expected_loss, seconds, status, computed_at "
        f"FROM lake.gold.sim_manifest "
        f"WHERE scenario = '{action['scenario']}' AND source_quarter IN ({quarters_list})"
    )
    manifest_result = db_client.get_db_client().run_query(manifest_sql)

    audit_log.log_event(
        tool="run_scenario_simulation", scenario=action["scenario"], quarters=action["quarters"],
        force=action["force"], real_command=" ".join(action["command"]), confirmed_by_user=True,
        exit_code=proc.returncode, duration_s=duration_s, status="ok" if proc.returncode == 0 else "error",
    )
    return {
        "exit_code": proc.returncode,
        "duration_s": duration_s,
        "stdout_tail": _clean_output(proc.stdout + proc.stderr),
        "manifest_columns": manifest_result["columns"],
        "manifest_rows": manifest_result["rows"],
    }
