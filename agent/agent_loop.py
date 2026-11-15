"""
The tool-calling loop against the Qwen (OpenAI-compatible) endpoint, plus the human-confirmation
re-entry point. `confirm()` is the only caller of run_scenario_tool._execute_confirmed_run — the
LLM itself never has a code path to it (see tool_specs.py).
"""
import json

from openai import OpenAI

import config
from schema_context import SYSTEM_PROMPT
from tool_specs import DISPATCH, TOOL_SPECS

MAX_ITERATIONS = 6


class AgentSession:
    def __init__(self):
        self.client = OpenAI(base_url=config.LLM_BASE_URL, api_key=config.LLM_API_KEY)
        self.messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        self.last_pending_action_id = None  # set whenever run_scenario_simulation proposes a plan
        self.last_tool_calls = []  # [{"name", "args", "result"}, ...] from the most recent ask(), for the UI

    def _call_model(self):
        return self.client.chat.completions.create(
            model=config.LLM_MODEL, messages=self.messages, tools=TOOL_SPECS, tool_choice="auto"
        )

    def _run_tool(self, tool_call) -> dict:
        func = DISPATCH.get(tool_call.function.name)
        if func is None:
            return {"error": f"Unknown tool {tool_call.function.name!r}."}
        try:
            args = json.loads(tool_call.function.arguments)
        except json.JSONDecodeError:
            return {"error": "Could not parse tool arguments as JSON."}
        try:
            return func(**args)
        except Exception as e:  # noqa: BLE001 - surfaced to the model, never crashes the session
            return {"error": str(e)}

    def ask(self, user_message: str) -> str:
        self.messages.append({"role": "user", "content": user_message})
        self.last_tool_calls = []

        for _ in range(MAX_ITERATIONS):
            response = self._call_model()
            msg = response.choices[0].message

            if not msg.tool_calls:
                self.messages.append(msg.model_dump(exclude_none=True))
                return msg.content

            self.messages.append(msg.model_dump(exclude_none=True))
            for tool_call in msg.tool_calls:
                result = self._run_tool(tool_call)
                try:
                    args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError:
                    args = {}
                self.last_tool_calls.append({"name": tool_call.function.name, "args": args, "result": result})
                if tool_call.function.name == "run_scenario_simulation" and "pending_action_id" in result:
                    self.last_pending_action_id = result["pending_action_id"]
                self.messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(result, default=str),
                })

        return "Sorry — I couldn't finish this within the allowed number of steps."

    def confirm(self, pending_action_id: str) -> str:
        from tools.run_scenario_tool import _execute_confirmed_run

        result = _execute_confirmed_run(pending_action_id)
        if self.last_pending_action_id == pending_action_id:
            self.last_pending_action_id = None
        self.messages.append({
            "role": "user",
            "content": (
                "The human confirmed. Here is the real execution result — summarize it in plain "
                f"English: {json.dumps(result, default=str)}"
            ),
        })
        response = self._call_model()
        msg = response.choices[0].message
        self.messages.append(msg.model_dump(exclude_none=True))
        return msg.content
