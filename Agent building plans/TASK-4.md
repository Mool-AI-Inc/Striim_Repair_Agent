# TASK 4 — Give the agent hands

**The problem, in one line:** the agent can see that something is wrong and
cannot find out *what*, because it only reads `/health`. Everything it needs
is reachable through doors that already work — nobody has wired them.

Concrete example from live testing. Striim's UI showed:

> `duplicate key value violates unique constraint "target_pkey"`
> `Key (id)=(9) already exists`

The agent said:

> "Inspect exception store rows to find rejection cause"

It told a human to go and read something **it could have read itself**.

---

## Priority 0 — Fix the message

Observed live. This is confusing and it is the thing an audience reads:

```
🙋  sarah1_CDC — Target rejected rows, pipeline halted (needs your OK)
    CDC stopped after target refused several incoming rows.
    ⚠ Don't restart: Inspect exception store rows to find rejection cause
    → Do this: Inspect exception store rows to find rejection cause
    medium confidence · Exception store has rejections; source and target
    both went silent together · target
    couldn't check: Actual rejected row payloads and error reason
```

Four faults:

1. **Two lines say the same thing.** "Don't restart" must give a *reason*;
   "Do this" gives the *action*. They can never be identical.
2. **It asks a human to do the agent's job.** "Inspect exception store rows"
   is a read the agent can perform.
3. **The footer is machine noise.** `medium confidence · ... · target` reads
   like a debug line, because it is one.
4. **Wrong shape for an auto-repair agent.** The headline should usually be
   what was FIXED, not what needs approval.

### The shape

Answer, in order: **what broke → what I did → what you must do.**
Nothing else on the first screen.

**When the agent repaired it (the common case):**

```
✅  Traill — fixed

    The target table "public"."target" had been renamed away, so every
    write failed. It is back now.

    I did:  deleted 4 duplicate rows blocking the resume, then resumed
    Result: 1,204 rows written, checkpoint advanced, running normally
```

**When it needs a human — and it always says exactly what:**

```
🙋  Traill — needs one command from you

    The writer expects a "city" column that no longer exists on the target.
    Everything else is ready; I cannot execute DDL.

    Run this on neondb (the target):

        ALTER TABLE "public"."target" ADD COLUMN "city" text;

    I will resume and verify as soon as it exists.
```

**When it deliberately did not act:**

```
🔴  Traill — stopped, not safe to restart

    The replication slot "striim_slot" no longer exists, so there is no
    position to resume from. Restarting would re-read from the beginning
    and duplicate everything already delivered.

    You need to: recreate the slot and run a fresh initial load.
```

### Rules

- **Plain English. No field names, no tool names, no confidence scores, no
  zone labels.** If it does not help a half-asleep person decide what to do,
  it is not on the message.
- **Every noun is specific.** The table, the column, the slot, the row id —
  never "the source system" or "several rows".
- **Numbers wherever there are numbers.** "4 duplicate rows", "1,204 written".
- **Never instruct a human to look something up the agent can read.**
- **Never repeat a line.** Reject any verdict where two fields match and
  re-ask the model.
- Detail belongs in **thread replies**, not the parent. The parent is what
  someone reads on a phone at 3am.

Put confidence, evidence and the tool trace in a reply, not the headline.

## What already works and is not being used

Both of these are proven on the live instance.

### The console endpoint

```python
requests.post(f"{STRIIM_URL}/api/v2/tungsten",
              headers={"Authorization": f"token {token}",
                       "Content-Type": "text/plain"},
              data="DESCRIBE ns.App_FileReaderSource;")
```

`describe_component` uses this. **Nothing else does.** The same door runs
`MON`, `SHOW`, `SELECT`, `ALTER`, `RESUME`, `START`, `STOP`, `DEPLOY`.

### A direct database connection

```python
psycopg2.connect(host=..., port=5432, dbname=..., user=..., password=...,
                 sslmode="require")
```

Verified working against the Neon target. The agent has no database access
at all today.

---

## Priority 1 — Read the exception store rows

**This single change is worth more than the rest of this document.** It turns
every "go look at the exception store" escalation into a named row.

```python
def get_rejected_rows(striim, app, limit=5):
    """The ACTUAL rejected events, not just whether any exist.

    get_rejected_events tells you the store has received rows. This tells
    you which rows and why - which is the difference between an alert and
    a diagnosis.
    """
    store = f"{app}_ExceptionStore"
    raw = console(striim, f"SELECT * FROM {store} LIMIT {limit};")
    ...
```

Run it through the console door. If `SELECT` is refused there, try the
app's monitoring endpoint — the web UI renders this text, so it is served
from somewhere and is worth twenty minutes to find.

**Expected effect on the message:**

| Before | After |
|---|---|
| "Inspect exception store rows to find rejection cause" | "Row id=9 rejected: duplicate key. 3 more like it." |

Wire it into the diagnosis loop as a tool, and update the prompt: **never
tell a human to inspect the exception store — read it.**

---

## Priority 2 — `MON` and the rest of the console

Add a general read-only console tool plus these wrappers:

| Tool | Command | Answers |
|---|---|---|
| `mon_app(app)` | `MON <app>;` | live per-component rates, queue depths, states |
| `mon_component(name)` | `MON <component>;` | is this one component the bottleneck |
| `show_checkpoint(app)` | `SHOW <app> CHECKPOINT HISTORY;` | is the position advancing, not just existing |
| `report_latency(app)` | `REPORT LATENCY ...;` | which component is the bottleneck |
| `usage(app)` | `USAGE <app>;` | volumes handled |
| `preview(cq)` | `PREVIEW <cq> INPUTOUTPUT LIMIT 5;` | what is flowing through a step |
| `show_stream(name)` | `SHOW <stream>;` | is a stream full — backpressure |
| `list_components(app)` | `LIST ...;` | what the app is actually made of |
| `console_read(stmt)` | any read-only statement | escape hatch for what we have not anticipated |

**Use everything that is available.** The agent must not settle for a vague
diagnosis while an unread command could answer the question. If a tool exists
and would narrow the cause, call it. The prompt should say this explicitly:
*exhaust the read tools before concluding a cause is undeterminable, and if
you still cannot tell, name the specific command whose output you would
need.*

The same applies to what the operator has supplied. Source and target
connection details are given deliberately so the agent can look at both ends
of the pipeline, not just Striim's opinion of them. A diagnosis that ignores
an available database connection is an incomplete diagnosis.

**Guardrail:** `console_read` must reject anything that is not a read.
Allowlist the leading verb — `MON`, `SHOW`, `SELECT`, `DESCRIBE`, `LIST`,
`STATUS`, `PREVIEW`, `USAGE`. Everything else goes through the action layer
where the namespace lock and audit log live.

`MON` returns human-formatted tables. Parse them into JSON — do not hand
box-drawing characters to the model.

---

## Priority 3 — Direct database access

Credentials come from the **environment**, never extracted from Striim's
stored config. We do not harvest secrets from the platform.

```
TARGET_DB_URL=postgresql://user:pass@host:5432/db?sslmode=require
SOURCE_DB_URL=postgresql://...        # optional, READ ONLY
```

If a URL is not set, every tool below returns *"no database access
configured"* — explicitly, never a guess. The agent must work without them,
just less well.

### 3.1 Read tools (both source and target)

| Tool | Purpose |
|---|---|
| `db_probe(which)` | `SELECT 1` — **is it actually back?** The single biggest gap today |
| `db_table_exists(which, table)` | did someone rename or drop it |
| `db_row_count(which, table)` | how much is actually there |
| `db_describe_table(which, table)` | columns, types, primary key, constraints |
| `db_replication_slot_status()` | source only: slot exists, `active`, `confirmed_flush_lsn` advancing |
| `reconcile(source_table, target_table)` | counts on both sides and the drift |

`db_probe` is what unlocks autonomy. Every HALT so far has ended in refusal
because the agent could not tell whether the cause had cleared. With a probe
it can.

`db_replication_slot_status` matters as much for Postgres pipelines: the
position lives in the slot, not only in Striim's checkpoint. A slot that is
`active = false` while the app claims RUNNING is a fault nothing currently
detects — observed live during testing.

`reconcile` answers the question raised by our own finding that Striim's
counters measure work performed, not data present. Read-only on both sides.

### 3.2 Write tools — TARGET ONLY

Source database access is **read-only, always**. No exceptions, enforced by
using a separate connection whose tool set contains no write verbs.

**On the target the agent has full execution rights.** This is an auto-repair
pipeline: it fixes what it can reach and only stops where it genuinely
cannot act. There is no row-count gate — if the diagnosis names a cause and
the statement addresses that cause, it runs.

What it still must do every time, because these are what make an autonomous
write defensible rather than reckless:

- **a named cause** in the verdict that this specific write addresses. Never
  a speculative write.
- **count first, on the same predicate.** `SELECT COUNT(*)` with the
  identical `WHERE` as the statement. The number goes in the incident.
- **the exact statement in the thread**, before or with execution. Never a
  silent write.
- **bounded `WHERE`.** No unqualified `DELETE`, no `TRUNCATE`, no `DROP`.
- **audit log**: statement, row count, timestamp, incident id.
- **verify afterwards** — did the write have the effect the plan predicted?

### 3.3 The one thing it cannot execute: DDL

The agent has every right here **except execution**. It works out exactly
what needs to run and says so; a human runs it.

That means a DDL proposal is not a description. It is the statement,
finished, correct, and ready to paste:

```
🙋  Traill — target column missing (needs you)
    The writer expects a "city" column that no longer exists on the target.
    → Run this on the target database:

    ALTER TABLE "public"."target" ADD COLUMN "city" text;

    Derived from the source schema. I cannot execute DDL - everything else
    about this repair is ready, and I will resume the pipeline once the
    column is back.
```

**Requirements for every propose-only output:**

1. **The literal statement**, correct for the dialect, fully qualified,
   copy-pasteable. Not "add the missing column".
2. **Where to run it** — which database, which connection.
3. **What was checked to derive it** — the source schema, the writer's
   expected columns, whatever it read.
4. **What happens next** — the agent is waiting and will resume once done,
   and will verify afterwards.
5. **The reversal**, where one exists, so a human can undo it.

The same applies to anything else the agent cannot run itself. Never
"inspect X" or "check Y" — always the specific command, and the reason it
could not be run.

## Priority 4 — Close the loop

The agent currently diagnoses and stops. It should carry a repair through.

```
DIAGNOSE → FIX THE CAUSE → RESUME → VERIFY → CLOSE
```

### 4.1 Component property changes

Several real faults are one property away from fixed, and all are pipeline
zone — squarely within the agent's rights:

| Fault seen live | Fix |
|---|---|
| target Tables set to `"public"."%",public.%` — writes into its own source | set explicit `public.source,public.target` |
| events buffered, nothing flushing (3 in, 1 out, 0 discarded) | lower the writer's batch policy |
| same recoverable exception every restart | set `IgnorableExceptionCode` — executes, but the incident MUST state how many rows this will discard, read from the exception store first |

Needs `alter_component_property(component, property, value)`, which requires
the full cycle:

```
STOP → UNDEPLOY → ALTER → DEPLOY → START
```

That is five commands with failure possible at each. Implement it as one
guarded operation that reports which step failed, and **snapshot the old
value first** so it can be described in the incident and reverted.

### 4.2 The repair loop

```python
def repair(state):
    # 1. is the cause actually gone?
    if not cause_cleared(state):        # db_probe, table_exists, slot_status
        return escalate("cause still present: <specific thing>")

    # 2. fix what is in our zone
    if state.fault_zone == "target" and plan.is_safe:
        apply_target_repair(plan)       # counted, logged, posted

    # 3. resume
    resume(state.app)

    # 4. verify - data moved, not just status
    return verify(state.app, snapshot_before)
```

Step 1 is the one that does not exist today and is why nothing gets repaired.

### 4.3 Retry discipline

- one repair attempt per incident fingerprint
- if verification fails, **escalate — do not try again**
- budget still applies
- every attempt in the thread, whether it worked or not

---

## Priority 5 — Pre-flight

Two of the faults hit during setup were misconfigurations present from
creation, not runtime failures. Both were detectable before deploy:

| Check | Catches |
|---|---|
| source table == target table in the mapping | self-replication loop |
| replication slot exists, is active, plugin matches the adapter | the wal2json/pgoutput mismatch |
| target table exists and its PK accepts explicit values | the `GENERATED ALWAYS` rejection |
| batch policy vs expected volume | events buffered forever |

Expose as `preflight(app)` — run on demand, and automatically the first time
an app is monitored. Never blocks anything; it reports.

---

## Acceptance tests

| # | Scenario | Expected |
|---|---|---|
| 1 | Duplicate-key rejection | incident names the row and the constraint |
| 2 | Target table renamed away | probe says missing, refuses, names the table |
| 3 | Target table renamed back | probe says present → resume → verify → ✅ |
| 4 | Replication slot dropped | names the slot, says fresh initial load needed, refuses |
| 5 | Slot `active=false` while app RUNNING | detected and reported |
| 6 | No `TARGET_DB_URL` set | every db tool says "not configured", agent still works |
| 7 | `console_read("DROP TABLE x")` | refused by the verb allowlist |
| 8 | Target repair, any size | executed, with the count and statement posted first |
| 9 | Source-side write attempted | impossible — no such tool exists |
| 10 | Wildcard table mapping | pre-flight flags it before deploy |

Tests 3 and 4 are beat 5 and skill 7. Tests 6, 7 and 9 are the safety
boundary. Prioritise those five.

---

## Order

1. **Exception store rows** — one tool, transforms every message
2. **`db_probe`** — one tool, unlocks autonomous repair
3. `mon_app` + `console_read`
4. `alter_component_property` + the repair loop
5. Target DML
6. Pre-flight

Items 1 and 2 are each a single tool and together they change what the agent
is capable of. If nothing else ships, ship those.
