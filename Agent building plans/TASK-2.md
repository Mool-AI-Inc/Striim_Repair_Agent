# TASK 2 — Validate, then finish Slack

**Context:** the build from `PLAN.md` is done and 40 tests pass offline.
Live credentials now work (`doctor` is green, 576 pipelines visible, 4 in
`sarah_moolai`). Real fixtures have been captured.

**The demo is tomorrow.** Everything below is ordered by what breaks the demo
if it is wrong. Do them in order. If time runs out, stop — a smaller thing
that works beats a larger thing that does not.

---

## Priority 1 — Prove the field names are real

The 40 passing tests were written against **synthesised** fixtures. They
prove the logic holds. They do **not** prove that the field names match what
Striim actually sends — and this project has already been bitten three times
by exactly that (see `PLAN.md` §6.1, §6.4, §6.5).

Real fixtures now exist:

```
tests/fixtures/trial5_halt.json      captured from a real HALT
tests/fixtures/trial5_running.json   captured healthy
```

Also capture and add:

```
python -m agent capture MANIMARAN22.OracleToFabricDWH_CDC
```

That one is read-only, already TERMINATED, and gives a **non-file,
non-owned** fixture — the only thing that makes the connector-independence
claim testable rather than asserted.

### Do this

1. Re-run the whole suite against the real fixtures, not the synthesised ones.
2. **Delete the synthesised fixtures** once real ones cover the same case.
   Two sets that disagree is worse than one set that is right.
3. Fix every field-name mismatch that surfaces. Do not paper over one by
   adding a fallback that reads both spellings — find out which is real and
   use that.
4. Add a test that asserts each tool returns its documented keys when handed
   `trial5_halt.json`. That is the test that would have caught all three of
   the historical bugs.

### Acceptance

- Suite passes against real fixtures only
- At least one fixture is a non-file connector from another namespace
- No synthesised fixtures remain for scenarios a real one covers

---

## Priority 2 — Finish Slack

This is the demo surface. Everything the audience sees happens here.

### 2.1 Move from webhook to bot token

The prototype used an incoming webhook, which can only post. Threads and
reactions need a bot token.

Environment:

```
SLACK_BOT_TOKEN=xoxb-...     chat:write, reactions:write
SLACK_APP_TOKEN=xapp-...     connections:write  (Socket Mode)
SLACK_CHANNEL=#striim-incidents
```

`doctor` already reports on these — make sure it distinguishes *"outbound
works, inbound does not"* from *"nothing configured"*, because that is a
state the demo may legitimately run in.

### 2.2 Threads — one per incident

The incident is a parent message. Everything that happens next is a **reply
in that thread**:

```
🔴  Trial5 — Source file went missing (needs you)          ← parent, ts saved
    └ 🔧 Restarting — cause looks cleared                   ← reply (ACT)
    └ ✅ Recovered — target wrote 412 new rows              ← reply (VERIFY)
```

Store `slack_thread_ts` in `IncidentState`. Every subsequent post for that
incident passes it as `thread_ts`.

**Why it matters:** whoever scrolls in at 8am sees the entire life of the
incident in one place, and an incident with no closing reply is visibly
still open.

### 2.3 Emoji as incident state

On the **parent** message only:

| State | Reaction |
|---|---|
| raised | 🔴 `red_circle` |
| agent acting | 🔧 `wrench` |
| resolved by agent | ✅ `white_check_mark` |
| needs a human | 🙋 `raising_hand` |

Agent adds and removes these itself via `reactions.add` / `reactions.remove`.
A human can also toggle them manually — do **not** fight a human who marks
something resolved. If a human adds ✅, treat the incident as closed and stop
posting to that thread.

### 2.4 Inbound commands (Socket Mode)

```
monitor sarah_moolai.Trial5      start watching
unmonitor sarah_moolai.Trial5    stop watching
status                           what am I watching, and how is each one
diagnose sarah_moolai.Trial5     one-off investigation, no watching
help                             list the commands
```

Rules:

- **Namespace check on `monitor`.** Reject anything outside the configured
  namespace with a clear message naming the namespace. `diagnose` may run on
  any pipeline (read-only) — this asymmetry is deliberate and is worth
  saying in the reply so the audience notices it.
- **Persist the watch list** (`registry.py` already exists). A restart of the
  agent must not lose what it was watching.
- **Acknowledge every command** within a second, even if the work takes
  longer. Silence looks broken on stage.
- **Unknown command → short help**, not silence.

### 2.5 Degradation

The agent must keep working when Slack does not.

| Missing | Behaviour |
|---|---|
| no bot token | print everything to terminal, carry on |
| no app token | outbound only, CLI control |
| Slack call fails mid-incident | print the message locally, log it, continue |

Never let a delivery failure stop the monitoring loop. A dropped message is
an annoyance; a stopped agent is a missed incident.

### Acceptance

- Incident + action + verification appear as one thread, not three messages
- Parent emoji changes as the incident progresses
- `monitor` from Slack starts a watch that survives an agent restart
- `monitor` on another namespace is refused; `diagnose` on it works
- Killing Slack connectivity mid-run does not stop the agent

---

## Priority 3 — Demo script

A `DEMO.md` with the exact sequence, the commands to type, what to say, and
the expected output at each step. Timed.

The five beats, in this order:

1. **The problem** — `doctor` output. 228 HALT, 61 TERMINATED out of 576, on
   Striim's own dev cluster. Their data, not our claim.
2. **Silence when healthy** — `monitor` a working pipeline from Slack.
   Nothing happens. Stop one deliberately. Still nothing.
3. **A real incident** — break the source file. Incident appears naming the
   file and the path.
4. **Refusal** — it does not restart, and says why: the checkpoint offset
   points into a file that is gone, so a restart re-reads everything and
   duplicates the target. No alert would ever fire for that.
5. **Repair** — restore the cause, agent restarts, verifies data actually
   moved, closes the thread with ✅.

Then the contrast slide: **Trial3 vs Trial5** — same connector, same fault,
opposite advice, because one has a usable checkpoint and the other does not.
Both outputs are already captured.

Include a **fallback path**: if the live cluster misbehaves on the day, the
same five beats must be runnable from the captured fixtures offline. Make
that a single command.

### Acceptance

- Whole demo runs in under 8 minutes
- Every step has an offline equivalent
- Nothing in it requires typing more than one command

---

## Do NOT do

Out of scope for tomorrow. Note as roadmap, do not build:

- UC2 schema-drift agent
- Interactive Slack buttons (needs a public callback URL)
- Multi-agent orchestration
- Reading exception-store row payloads
- DDL execution — propose-only stands, as you correctly narrowed it

---

## Notes on your three judgement calls

All three accepted, no changes needed:

- **ESCALATE before ACT** — right, and better than the diagram. The thread
  parent must exist before replies can attach, and immediate visibility beats
  a four-minute silence.
- **Evidence-based checkpoint check** — right, and better than the byte
  threshold that was specified. Using *which component failed* to decide
  whether the position points into something missing is the correct
  reasoning, and it makes the Trial3/Trial5 contrast emerge rather than be
  hardcoded. Keep test 4 (model says "restart, high confidence", code
  overrules) — it demonstrates the central design claim and belongs in the
  demo.
- **DDL propose-only** — right. §5 contradicted itself: rule 4 requires a
  `WHERE` clause and DDL cannot have one. Propose-only resolves it safely.
