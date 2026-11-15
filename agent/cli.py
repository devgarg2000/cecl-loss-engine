"""
Phase 1 entrypoint: a plain terminal chat loop against the agent. No web UI yet (Phase 3).

Run from the repo root:
    .venv\\Scripts\\python.exe agent\\cli.py
"""
import sys

from agent_loop import AgentSession


def main():
    sys.stdout.reconfigure(encoding="utf-8")  # Windows console defaults to cp1252; the model uses em-dashes etc.
    print("CECL portfolio agent (Phase 1 CLI). Type 'exit' to quit.\n")
    session = AgentSession()

    while True:
        try:
            user_input = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not user_input:
            continue
        if user_input.lower() in {"exit", "quit"}:
            break

        reply = session.ask(user_input)
        print(f"\nagent> {reply}\n")

        if session.last_pending_action_id:
            answer = input(
                f"The agent proposed a real action (pending_action_id={session.last_pending_action_id}). "
                "Run it for real? [y/N] "
            ).strip().lower()
            if answer == "y":
                confirmed_reply = session.confirm(session.last_pending_action_id)
                print(f"\nagent> {confirmed_reply}\n")
            else:
                session.last_pending_action_id = None
                print("Not confirmed — nothing real was run.\n")


if __name__ == "__main__":
    sys.exit(main())
