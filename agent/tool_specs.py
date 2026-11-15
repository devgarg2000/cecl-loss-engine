"""
The exactly-3 tools the LLM can see and call. `_execute_confirmed_run` is deliberately NOT here —
that is the structural guarantee that the model can never run a real Spark job on its own.
"""
from tools.explain_tool import explain_scenario
from tools.run_scenario_tool import run_scenario_simulation
from tools.sql_tool import sql_query

TOOL_SPECS = [
    {
        "type": "function",
        "function": {
            "name": "sql_query",
            "description": (
                "Run a read-only SQL SELECT against the lake's gold tables to answer a question "
                "about the loan portfolio or simulation results. SELECT-only; will be rejected "
                "if it is anything else."
            ),
            "parameters": {
                "type": "object",
                "properties": {"sql": {"type": "string", "description": "A single SQL SELECT statement."}},
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_scenario",
            "description": (
                "Get the real segment-level loss breakdown for a scenario (optionally one quarter), "
                "so you can explain which segments drove the loss. Only narrate the numbers returned."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scenario": {"type": "string", "enum": ["baseline", "adverse", "severely_adverse"]},
                    "segment_by": {"type": "string", "enum": ["credit_band", "ltv_band"], "default": "credit_band"},
                    "quarter": {"type": "string", "description": "e.g. '2023Q1'. Omit for all quarters."},
                },
                "required": ["scenario"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_scenario_simulation",
            "description": (
                "Propose running the real simulation engine for a scenario and a small set of "
                "origination quarters. This ONLY proposes a plan (it runs a safe --dry-run first) — "
                "it never executes the real job. A human must separately and explicitly confirm "
                "before anything real runs. Always tell the human this and ask for their confirmation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "scenario": {"type": "string", "enum": ["baseline", "adverse", "severely_adverse"]},
                    "quarters": {
                        "type": "array", "items": {"type": "string"},
                        "description": "e.g. ['2023Q1']. Defaults to one quarter if omitted; capped at 3.",
                    },
                    "force": {
                        "type": "boolean", "default": False,
                        "description": (
                            "All quarters are already computed, so a normal run usually finds "
                            "nothing to do. Set true only if the human wants to force a real recompute."
                        ),
                    },
                },
                "required": ["scenario"],
            },
        },
    },
]

DISPATCH = {
    "sql_query": sql_query,
    "explain_scenario": explain_scenario,
    "run_scenario_simulation": run_scenario_simulation,
}
