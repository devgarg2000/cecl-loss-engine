"""
Phase 2 entrypoint: the same AgentSession/tool logic, exposed over HTTP.

Run from the repo root:
    .venv\\Scripts\\python.exe -m uvicorn agent.server:app --port 8800

uvicorn imports this as the package module `agent.server`, which puts the repo root (not
agent/) on sys.path — but every other agent/*.py file uses flat imports (`import config`, etc.)
written for cli.py's script-style invocation, where Python puts agent/ itself on sys.path.
This one line reconciles the two invocation styles without changing any of those files.
"""
import os
import sys
import threading
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from pydantic import BaseModel  # noqa: E402

import config  # noqa: E402
from agent_loop import AgentSession  # noqa: E402

app = FastAPI(title="CECL Portfolio Agent")

SESSIONS: dict[str, AgentSession] = {}
RUNS: dict[str, dict] = {}  # run_id -> {"status": "running"|"done"|"error", "reply": str | None}


def _get_session(session_id: str) -> AgentSession:
    session = SESSIONS.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Unknown session_id {session_id!r}.")
    return session


class ChatRequest(BaseModel):
    session_id: str
    message: str


class ConfirmRequest(BaseModel):
    session_id: str
    pending_action_id: str


@app.post("/api/session")
def create_session():
    session_id = uuid.uuid4().hex[:12]
    SESSIONS[session_id] = AgentSession()
    return {"session_id": session_id}


@app.post("/api/chat")
def chat(req: ChatRequest):
    session = _get_session(req.session_id)
    reply = session.ask(req.message)
    return {
        "reply": reply,
        "pending_action_id": session.last_pending_action_id,
        "tool_calls": session.last_tool_calls,
    }


def _run_confirm_in_background(run_id: str, session: AgentSession, pending_action_id: str):
    try:
        reply = session.confirm(pending_action_id)
        RUNS[run_id] = {"status": "done", "reply": reply}
    except Exception as e:  # noqa: BLE001 - surfaced via run-status, never crashes the thread silently
        RUNS[run_id] = {"status": "error", "reply": str(e)}


@app.post("/api/confirm")
def confirm(req: ConfirmRequest):
    session = _get_session(req.session_id)
    run_id = uuid.uuid4().hex[:12]
    RUNS[run_id] = {"status": "running", "reply": None}
    thread = threading.Thread(
        target=_run_confirm_in_background, args=(run_id, session, req.pending_action_id), daemon=True
    )
    thread.start()
    return {"run_id": run_id}


@app.get("/api/run-status/{run_id}")
def run_status(run_id: str):
    run = RUNS.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Unknown run_id {run_id!r}.")
    return run


@app.get("/api/audit")
def audit(limit: int = 50):
    if not os.path.exists(config.AUDIT_LOG_PATH):
        return {"events": []}
    with open(config.AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()[-limit:]
    import json
    return {"events": [json.loads(line) for line in lines]}


# Mounted last so /api/* routes above always match first; this serves the Phase 3 frontend.
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
