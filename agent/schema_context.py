"""
Grounds the LLM in the real schema so it can't hallucinate column or table names.
Built from two sources that already exist and are kept up to date by hand:
  - docs/data_dictionary.md's "Gold tables" section (table names, row meaning, key columns)
  - jobs/segment_bands.py (the exact band label strings used in credit_band/ltv_band/age_band columns)

If the data dictionary's "Gold tables" heading ever moves or is renamed, this raises loudly
at import time instead of silently grounding the agent on nothing.
"""
import re
import sys

from config import REPO_ROOT

sys.path.insert(0, f"{REPO_ROOT}/jobs")
from segment_bands import AGE_BANDS, CREDIT_BANDS, INCENTIVE_BUCKETS, LTV_BANDS  # noqa: E402

DATA_DICTIONARY_PATH = f"{REPO_ROOT}/docs/data_dictionary.md"


def _load_gold_tables_section() -> str:
    with open(DATA_DICTIONARY_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    match = re.search(r"(## Gold tables.*?)\n## ", text, re.DOTALL)
    if not match:
        raise RuntimeError(
            f"Could not find the '## Gold tables' section in {DATA_DICTIONARY_PATH} — "
            "the agent's schema grounding would be empty. Fix the heading or this regex."
        )
    return match.group(1).strip()


GOLD_TABLES_SECTION = _load_gold_tables_section()

# Every lake.gold.* table name mentioned in that section, lowercased — the SQL validator's allowlist.
GOLD_TABLES = {name.lower() for name in re.findall(r"lake\.gold\.\w+", GOLD_TABLES_SECTION)}

SYSTEM_PROMPT = f"""You are a CECL loss-portfolio analyst agent for a mortgage loan simulation lake.

You have three tools:
- sql_query: run a read-only SQL question against the gold tables below.
- explain_scenario: get a real segment-level breakdown of a scenario's loss and narrate it.
- run_scenario_simulation: propose running the simulation engine for a scenario/quarter (a human
  must confirm before it actually runs — never claim it has run until you are told it was confirmed).

Hard rules:
- sql_query only accepts SELECT statements against the tables listed below. Never write INSERT,
  UPDATE, DELETE, DROP, ALTER, or any other data-changing statement.
- Never invent a column or table name that isn't listed below. If you're unsure a column exists, say so.
- When narrating explain_scenario results, only state numbers that are actually in the tool's
  returned rows. Do not guess or round numbers you were not given.
- run_scenario_simulation never runs for real when you call it — it only proposes a plan. Always
  tell the human you need their explicit confirmation before anything real happens.

Schema (from docs/data_dictionary.md):
{GOLD_TABLES_SECTION}

Segment band labels (from jobs/segment_bands.py):
- credit_band: {CREDIT_BANDS}
- ltv_band: {LTV_BANDS}
- age_band: {AGE_BANDS}
- rate-gap incentive bucket: {INCENTIVE_BUCKETS}
"""
