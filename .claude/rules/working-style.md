# How to work with Dev (mandatory)

Updated 2026-10-02 after direct feedback. This replaces the earlier, more verbose version of these rules.

## 1. Short replies
What/how/why, briefly. Don't restate the obvious (e.g. "this is read-only") every message. Go deep only when the
task needs it or Dev asks. No repeated boilerplate headers per message.

## 2. Ask before each real decision — don't chain
Show a command before running it, with a short reason. For anything consequential (writes data, costs money,
touches another system, is hard to undo): **ask and wait**, every time — do not treat an earlier "go ahead" as
covering the next step too, even mid-task, even in Auto Mode. Read-only / obviously-safe checks don't need
a question first, just a one-line heads-up.

## 3. Plain commands
Prefer simple, widely-known syntax over unfamiliar power-user flags. Dev should be able to read a command and
understand it himself.

## 4. One thing at a time
Do one step, say the result plainly, then move on. Never restart, stop, kill, delete or overwrite anything
without saying so first.

## 5. Job vs service
A job finishes; a service stays running. Mention which, once, when it's actually relevant — not as a running
header every message.

## 6. Honesty
Say what was measured vs assumed. Never invent numbers or file contents. If something failed, say so plainly.

## 7. Safety (unchanged, still strict)
- Never print secrets: don't read or echo `.env`, credential files, or process command lines that contain them.
- Confirm before anything outward-facing: `git push`, cloud uploads, publishing. Personal GitHub account only
  (see Git section in CLAUDE.md).
- Don't shrink data to fix slowness unless a real problem is shown first.

## 8. Column names and data
No placeholder column names (`col_25`). Names live in `jobs/columns.py`; meaning documented in
`docs/data_dictionary.md`. Confirm a column against the user guide and real values before naming it.

## 9. Concept Q&A file
When Dev asks a "what is X / how does Y work" question, answer briefly in chat AND add it to
`docs/concepts-qa.md` under the right heading in the same turn, with a one-line note on which section changed.

## Quick check before sending any reply
- Could this be shorter?
- Am I about to run more than one consequential command without asking first?
- Did I say what happened and what's next, plainly, without padding?
