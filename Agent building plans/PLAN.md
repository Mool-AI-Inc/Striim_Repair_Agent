# Striim Pipeline SRE Agent — Build Plan

**For:** Claude Code
**Deliverable:** a live demo, run locally, presented to the Striim team
**Time budget:** one working day
**Status:** a working prototype already exists. This is a consolidation and
extension, **not** a rebuild from scratch.

---

## 0. Read this first

There is working code in this repo (`legacy/`). It was built by breaking a
real Striim instance repeatedly over several days, and it encodes a number of
non-obvious facts about how Striim actually behaves — several of which
contradict the documentation.

**Do not discard it and start again.** Port the logic, keep the guardrails,
and re-read §6 (Known Gotchas) before writing any HTTP call. Every item in
that section cost hours to discover.

---

## 1. What the agent is

A **control-plane** agent for Striim data pipelines. It sits beside the
pipelines, not in the data path. It reads Striim's control plane, works out
what is wrong when something breaks, fixes what it safely can, and escalates
what it cannot — with a written explanation a tired human can act on.

### The problem it solves

Striim already detects failures and already sends alerts. That part is
automatic. What is manual is everything after the alert:

| Step | Today |
|---|---|
| Detect it | automatic |
| Work out *why* | manual — a human logs in and runs `mon` |
| Decide the fix | manual |
| Apply the fix | manual (or a blind retry via AUTORESUME) |
| Confirm it worked | manual |

The alert says *what* broke. It never says *why*. One symptom has many
causes, and the fix differs for each. That gap is the product.

### What it is NOT

- Not a replacement for Striim's alerting — it consumes it.
- Not a data-path component. It never touches the events themselves.
- Not something that writes to a customer's target database. See §5.

---

## 2. Success criteria for the demo

The demo must show all five, in this order:

1. **Continuous monitoring.** Agent is told which pipelines to watch and
   keeps watching them without further input.
2. **Silence when healthy.** A running pipeline produces nothing in Slack.
   A pipeline someone stopped deliberately also produces nothing.
3. **A real incident, diagnosed.** Break a pipeline; within minutes an
   incident appears in Slack naming the specific cause — the actual file,
   the actual component — not "check the source system".
4. **Autonomous repair where safe.** A fault whose cause has cleared is
   restarted by the agent, then *verified* — and the verification checks
   that data actually moved, not merely that the status says RUNNING.
5. **Refusal where unsafe.** A fault where restarting would duplicate data
   or fail identically is NOT restarted. The agent explains why, and says
   exactly what a human must do.

Point 5 is the most important. Anyone can build something that restarts
pipelines — Striim ships that as a config flag. The product is knowing when
restarting is the wrong move.

---

## 3. Architecture

Build with **LangGraph**. The reason is legibility: an explicit state graph
makes the flow inspectable and auditable, which matters both for trust and
for the demo narrative. It also constrains the model — each node has a
narrow job, so there is less room to wander.

### The graph

```
                    ┌──────────┐
                    │  WATCH   │◄─────────────┐
                    └────┬─────┘              │
                         │ fault detected     │
                         ▼                    │
                  ┌─────────────┐             │
                  │   TRIAGE    │  is this real, and is it new?
                  └──────┬──────┘             │
              not real   │                    │
              ───────────┘ real               │
                         ▼                    │
                  ┌─────────────┐             │
                  │  EVIDENCE   │  wait for error text if needed
                  └──────┬──────┘             │
                         ▼                    │
                  ┌─────────────┐             │
                  │  DIAGNOSE   │  tool-calling loop, model drives
                  └──────┬──────┘             │
                         ▼                    │
                  ┌─────────────┐             │
                  │   DECIDE    │  guardrails in code, not the model
                  └──┬───┬───┬──┘             │
              tier 1 │   │   │ tier 3         │
                     │   │   └──────────┐     │
                     ▼   │ tier 2       ▼     │
              ┌─────────┐│        ┌──────────┐│
              │   ACT   ││        │ ESCALATE ││
              └────┬────┘│        └─────┬────┘│
                   ▼     ▼              │     │
              ┌─────────────┐           │     │
              │   VERIFY    │           │     │
              └──────┬──────┘           │     │
                     └──────────────────┴─────┘
                              report to Slack
```

### Node responsibilities

| Node | Job | Uses the model? |
|---|---|---|
| `WATCH` | poll status for each monitored pipeline | no |
| `TRIAGE` | is this a fault state? is it new, or already open? | no |
| `EVIDENCE` | if the fault has no error text yet, wait and re-check | no |
| `DIAGNOSE` | tool-calling loop — the model chooses what to read | **yes** |
| `DECIDE` | apply guardrails, choose tier | no |
| `ACT` | run the remediation command | no |
| `VERIFY` | did data actually move? | no |
| `ESCALATE` | write the incident, post it | formatting only |

**Only `DIAGNOSE` uses the model.** Everything else is deterministic. This
is the central design principle:

> **The model recommends. The code decides.**

The model can be wrong and nothing bad happens, because it has no hands.
Every guardrail lives in Python where it cannot be argued with.

### State carried through the graph

```python
class IncidentState(TypedDict):
    app: str
    status: str
    fingerprint: str          # dedupe key for this specific fault
    evidence: dict            # accumulated tool results
    verdict: dict | None      # the model's structured opinion
    tier: int | None
    action_taken: str | None
    verification: dict | None
    slack_thread_ts: str | None
```

---

## 4. Components to build

### 4.1 Tool layer (read-only) — `striim_tools.py`

Already exists and works. Port as-is. Every function is read-only; nothing
here can change anything on the cluster.

| Tool | Answers |
|---|---|
| `get_status(app)` | is it alive, and what does that state *mean* |
| `get_errors(app)` | the actual error text |
| `get_flow_stats(app)` | source/target rates + connector profile |
| `get_state_history(app)` | did a human do this deliberately |
| `get_checkpoints(app)` | is there a resumable position |
| `get_rejected_events(app)` | exception store — were rows refused |
| `describe_component(name)` | what it is configured to read, and where it got to |
| `list_apps(namespace)` | what exists and what is broken |

**Design rule for every tool:** it must be able to say *"I don't know"* as
distinct from *"no"*. An empty result is not evidence of absence. Three of
the five bugs found during prototyping were tools reporting missing data as
a negative finding, which led the model to confidently wrong conclusions.

### 4.2 Action layer (write) — `striim_actions.py`

The **only** file that can change anything. Keeping it small and separate
means "what can this agent do?" is answered by reading one short file.

Hard limits, enforced in code:

- **Namespace lock.** Reads are allowed cluster-wide; writes only within the
  configured namespace. (Reading other pipelines is how we demonstrate
  connector-independence; acting on them would be unacceptable.)
- **Verb allowlist.** Only `START`, `STOP`, `RESUME`, `DEPLOY`, `UNDEPLOY`.
- **`DRY_RUN` default on.** Set `STRIIM_LIVE=1` to act for real.
- **Budget.** Max 2 restarts per app per hour.
- **No repeats.** Never act twice on the same incident fingerprint without a
  human. A repeat is a pattern, and patterns need people.

### 4.3 Verification — the part most implementations get wrong

After acting, **"it says RUNNING" is not proof.** A pipeline can report
RUNNING while doing nothing, or fail again forty seconds later.

Verification must:

1. Snapshot *before* acting — without a baseline there is nothing to compare.
2. Wait out the monitoring lag (see §6) before reading anything back.
3. Check that **the target wrote new data** or **the checkpoint advanced**.
4. Return **three** outcomes, not two:
   - `True` — recovered, with evidence
   - `False` — still broken, escalate
   - `None` — healthy but nothing has moved yet; **cannot confirm**

Outcome `None` is common and correct — a file source that has already read
everything will sit healthy with nothing to prove. An agent that cannot say
"I don't know" ends up lying.

### 4.4 Slack interface — `slack_io.py`

Two directions.

**Outbound — incidents.** One channel (`#striim-incidents`), one **thread**
per incident. The alert is the parent; diagnosis, human replies, action taken
and verification result all land as replies underneath. Whoever scrolls in
sees the whole life of the incident in one place.

State shown by emoji on the parent: 🔴 raised → ✅ resolved. The agent sets
these itself; a human can also toggle manually.

Message format — five lines, readable on a phone, in the dark, half asleep:

```
🔴  Trial5 — Source file went missing (needs you)
    The file reader lost its source part way through reading it.
    ⚠ Don't restart: it will re-read the whole file and duplicate every row
    → Do this: restore AI.csv to UploadedFiles/sarah_moolai/
    high confidence · source unreadable, checkpoint offset invalid
```

Every field one short line, under 15 words, plain English, no tool names, no
jargon. If unsure, lower the stated confidence rather than hedging in prose.

**Inbound — control.** Slack is the operator console. Commands:

```
monitor sarah_moolai.Trial5      start watching a pipeline
unmonitor sarah_moolai.Trial5    stop watching it
status                           what am I watching, and how is it
diagnose sarah_moolai.Trial5     one-off investigation, no watching
```

Use **Socket Mode** (`slack_bolt`). It opens an outbound WebSocket to Slack,
so there is no public callback URL, no tunnel, and it runs fine from a laptop
behind a router. This is the whole reason inbound control is cheap here.

Tokens needed:
- **bot token** `xoxb-` with `chat:write`, `reactions:write`
- **app-level token** `xapp-` with `connections:write` (Socket Mode only)

There is no per-user auth to build. The agent holds one set of Striim
credentials; anyone in the channel is talking to that one operator identity.
A pipeline name in a message is just a string.

The handler is small:

```python
@app.message(re.compile(r"^monitor\s+(\S+)"))
def start_monitoring(message, context, say):
    app_name = context["matches"][0]
    registry.add(app_name)
    say(f"Now monitoring {app_name}")
```

**Persist the monitored list** to a small JSON file so a restart of the agent
does not lose what it was watching. Namespace-check names on `monitor` and
reject anything outside the configured namespace with a clear message.

### 4.5 CLI

```bash
python -m agent monitor sarah_moolai.Trial5 sarah_moolai.Trial3
python -m agent diagnose sarah_moolai.Trial5     # one-shot, no watching
python -m agent watch --namespace sarah_moolai   # everything in a namespace
```

---

## 5. Repair scope — what the agent may and may not touch

The pipeline sits between a source and a target. A fault in any of the three
halts it, but the agent's rights differ in each.

### Three zones

| Zone | Rights | What the agent does |
|---|---|---|
| **The pipeline itself** | full control | fix and resume autonomously |
| **The target** | DML allowed — we have write rights | corrective SQL, then resume |
| **The source** | **read only** | never modify; flag for human review |

### Zone 1 — the pipeline

Anything in Striim's own control plane: `START`, `STOP`, `RESUME`,
`DEPLOY`, component properties, TQL. The agent fixes these itself and
resumes immediately. Guardrails are the budget and no-repeat rules in §4.2.

### Zone 2 — the target

The agent **may run DML against the target** to repair damage that stops the
pipeline from running cleanly. Typical corrective work:

- delete duplicate rows left by an earlier re-read
- remove a partially-written batch so a resume does not collide with it
- clear a row the writer keeps rejecting, once diagnosed
- create a missing table or add a missing column the writer needs

**Required before any target write:**

1. **Diagnose first.** Never write to the target as a guess. There must be a
   named cause in the verdict that the write specifically addresses.
2. **Count before you change.** Run the equivalent `SELECT COUNT(*)` and put
   the number in the incident. "This will delete 412 rows" is the difference
   between an informed action and an accident.
3. **Show the SQL.** The exact statement goes into the Slack thread before or
   alongside execution, always. Never a silent write.
4. **Bounded scope.** Every statement must carry a `WHERE` clause tied to the
   diagnosed problem. No unqualified `DELETE`, no `TRUNCATE`, no `DROP`.
5. **Log everything.** Statement, row count, timestamp, and the incident it
   belongs to, written to a local audit file.

**Autonomy threshold** (make this a config value, default conservative):

- affects **≤ N rows** (default 1000) and matches an allowlisted corrective
  pattern → the agent executes, then resumes, then verifies
- affects **more than N rows**, or is `TRUNCATE`/`DROP`/schema-destructive, or
  the agent's confidence is below high → post the SQL and the row count to
  Slack and wait for a human

### Zone 3 — the source

**The agent never writes to the source.** Not a file, not a table, not a
credential. If the diagnosis lands on the source — missing file, unreachable
host, expired credential, dropped replication slot — the agent:

- names the exact thing that is wrong (`describe_component` gives the path,
  the host, the object)
- states what a human must do
- raises the incident as **human review needed** and stops

This is not caution for its own sake. The source is someone else's system of
record; a wrong write there is unrecoverable and invisible to us.

### Deciding which zone a fault belongs to

The diagnosis must classify the fault before any action is chosen. Add a
field to the verdict schema:

```json
"fault_zone": "pipeline" | "target" | "source" | "unknown"
```

- `unknown` behaves as `source` — no writes, escalate. Uncertainty resolves
  towards the safest zone, never the most convenient one.
- A fault can span zones (a poison row is a source-data problem that mainfests
  as a target rejection). When it does, classify by **what the agent would
  have to change to fix it**, not by where the error text came from.

### Note on Striim's own recovery

For many target-side failures no DML is needed at all: with `RECOVERY`
enabled, resuming replays from the checkpoint. Check the checkpoint before
reaching for SQL — but check it properly, because a checkpoint can exist and
be unusable (§6.4).

## 6. Known gotchas — all discovered the hard way

These are non-obvious and several contradict the documentation. Re-read
before writing any integration code.

### 6.1 The console endpoint auth is not what the docs say

`/api/v2/tungsten` requires a **plain-text body** and an
`Authorization: token <value>` header.

- JSON body → `415 Unsupported Media Type`
- `STRIIM-TOKEN` header → `401`

```python
requests.post(f"{STRIIM_URL}/api/v2/tungsten",
              headers={"Authorization": f"token {token}",
                       "Content-Type": "text/plain"},
              data="DESCRIBE ns.App_FileReaderSource;")
```

### 6.2 `/health` lags roughly two minutes

It is a cached cluster snapshot, not a live view. Detection cannot be faster
than this, and **verification must wait longer than this** or it will read
the pre-action world and declare a working fix a failure. Use ≥210s.

### 6.3 Error text is published separately from the status change

Poll too soon after a fault and you get a status with no reason attached,
which produces a useless diagnosis. Wait and re-check (45s × 4) before
diagnosing. If the fault is old and there is still no text, none is coming —
say so rather than telling the user to wait for it.

### 6.4 A checkpoint can exist and be unusable

Observed: recovery enabled, checkpoints recorded with real timestamps,
`has_usable_checkpoint: true` — and the recorded read offset was **170,969
bytes on a ~1KB file**. The offset had accumulated across file
replacements and no longer referred to the file present. Striim cannot seek
to it, so it re-reads from the start and duplicates everything.

**Therefore:** `has_usable_checkpoint == true` is *not* sufficient to
conclude a restart is safe. Call `describe_component` and sanity-check the
offset against plausible file size.

### 6.5 `lastModifiedTime` is not "last activity"

It is when the application *definition* was last edited. Using it to age a
fault invents a fault age out of nothing. Age faults from the state-change
timestamps instead.

### 6.6 Zero throughput means different things in different states

`DEPLOYED` (never started) and `COMPLETED` (finished normally) and a stalled
`RUNNING` all show zero. The status word alone cannot distinguish them.
Tools must state what each state *means*, not just report it.

### 6.7 The deliberate-shutdown test only applies one way

A clean shutdown passes through `STOPPING`. But `STOPPED → RUNNING` is a
*start*, and starts are always requested — the absence of `STOPPING` there
means nothing. Applying the test to a start is a category error.

### 6.8 The state-change list is a short rolling window

Entries age out. An empty list means "no data", **not** "not deliberate".

### 6.9 Striim only watches the reading side

Deleting the **target** file produces no alert, no discarded events, no
status change — the writer simply creates a new one. This is a genuine blind
spot and worth demonstrating.

### 6.10 In/out counters measure work done, not data present

Observed 9.9k in / 9.9k out reconciling perfectly while the target file held
~1.9k, because deletions happened outside Striim's view. **A matched in/out
count is not proof of correctness.**

---

## 7. Connector independence

The agent talks to Striim. Striim talks to everything else. **The tool layer
is the same size regardless of how many connectors are in play** — every
adapter reports through the same control plane, in the same `/health`
response, with the same component-naming convention.

What varies is not the tools but the *knowledge*: silence is normal for a
file reader that has finished, and suspicious for a CDC source.

### Connectors in scope

These are the source and target types the pipelines may use. The agent
must handle any of them without connector-specific code.

| Group | Connectors |
|---|---|
| **Relational / CDC** | Oracle, PostgreSQL, MySQL, MariaDB, SQL Server, Spanner |
| **Warehouses & lakes** | BigQuery, Snowflake, Microsoft Fabric DWH, BigLake, Iceberg, Databricks |
| **Object storage & files** | Amazon S3, local/SFTP files, OneDrive, Google Drive |
| **SaaS / API** | Salesforce, Workday, Zoho, Jira, Google Analytics, Google Ads, Adobe Analytics, HubSpot, Shopify, NetSuite |
| **Spreadsheets** | Google Sheets, Excel Online |
| **Event streams** | Kafka, event buses |
| **BI / messaging targets** | Tableau, Slack, HTTP writers |

### Profiles are keyed by BEHAVIOUR, not by product

Writing one entry per product would mean forty entries and constant
maintenance. Group by how the connector behaves, because that is what
actually determines the right diagnosis:

| Behaviour class | Members | Silence normal? | Restart risk |
|---|---|---|---|
| **CDC reader** | Oracle, Postgres, MySQL, MariaDB, SQL Server, Spanner (change capture) | **No** — a live DB is rarely silent for long | resumes from stored position (SCN/LSN/binlog) **if recovery is on**; if the log has been purged past that position, it cannot resume |
| **Batch / initial-load reader** | same databases in initial-load mode, warehouse readers | Yes — runs once and completes | may re-run the whole query and duplicate results |
| **File-like reader** | files, S3, SFTP, OneDrive, Drive | Yes — reads to end, then idles | **depends on the checkpoint** — see §6.4; if unusable, a restart re-reads everything and duplicates |
| **API poller** | Salesforce, Workday, Zoho, Jira, GA, Ads, NetSuite | Yes — quiet outside business hours; also rate-limited | may re-consume API quota; token expiry is a common cause |
| **Spreadsheet reader** | Google Sheets, Excel Online | Yes | whole-sheet re-read is likely; also breaks on manual edits and column reordering |
| **Stream reader** | Kafka, event buses | **No** — sustained silence is suspicious | resumes from offset if committed; risk of gap if retention has passed |
| **Transactional writer** | relational targets | n/a | partial batch may be half-applied — this is where target DML repair applies (§5 zone 2) |
| **Append-only writer** | files, S3, warehouses, lakes | n/a | **appends, never replaces** — this is why a re-read duplicates silently |

### Implementation

```python
BEHAVIOUR = {
    "OracleReader": "cdc_reader",
    "PostgresReader": "cdc_reader",
    "MysqlReader": "cdc_reader",
    "FileReader": "file_reader",
    "S3Reader": "file_reader",
    "SalesforceReader": "api_poller",
    "GoogleSheetsReader": "spreadsheet_reader",
    "KafkaReader": "stream_reader",
    # ... adapter name -> behaviour class
}

PROFILES = {
    "file_reader": {
        "silence_is_normal": True,
        "why": "reads to the end of its input, then idles with nothing to do",
        "restart_risk": "DEPENDS ON THE CHECKPOINT — call get_checkpoints and "
                        "describe_component before judging. If the recorded "
                        "position is missing or implausible, a restart "
                        "re-reads everything and duplicates every row.",
    },
    "cdc_reader": {
        "silence_is_normal": False,
        "why": "change capture on a live database; long silence is suspicious",
        "restart_risk": "resumes from the stored log position if recovery is "
                        "on. If the source log has been purged past that "
                        "position, it cannot resume — escalate.",
    },
    # ...
}
```

Resolve the adapter name from the component name (Striim names components
after their adapter: `Trial5_FileReaderSource`, `X_OracleSource`), map it to
a behaviour class, attach the profile to `get_flow_stats` output.

**Unknown adapter → return an explicit "unrecognised" profile**, not a
default. Do not silently assume file-reader behaviour for something that
might be a CDC source. Say "I do not know how this connector behaves" and let
the diagnosis account for that.

### Write the condition, not the conclusion

A flat `"restart_risk": "HIGH"` drowns out contradicting evidence from other
tools — the model repeats the loudest input instead of reconciling them. This
was observed during prototyping: the file-reader profile said HIGH RISK
unconditionally, and the agent kept warning about duplication even on a
pipeline that had a perfectly valid checkpoint.

State the condition and name the tool that resolves it, as in the example
above.

### Demo this

The shared dev cluster is full of other people's broken Oracle, Salesforce
and BigQuery pipelines. Point the agent at one, read-only. It diagnoses a
connector it has never seen, and gives connector-appropriate advice — during
prototyping it correctly raised Oracle SCN positioning without being told
anything about Oracle. That kills the "does this only work for files?"
question before it is asked.

## 8. Acceptance tests

Each must pass before the demo.

| # | Scenario | Expected |
|---|---|---|
| 1 | Healthy pipeline | nothing posted to Slack |
| 2 | Human stops a pipeline | nothing posted — `STOPPING` seen, not a fault |
| 3 | Source file deleted mid-read | incident within ~5 min naming the file and path |
| 4 | Same, with no usable checkpoint | refuses restart, explains duplication risk |
| 5 | Fault whose cause has cleared, valid checkpoint | restarts, verifies, posts ✅ |
| 6 | Restart that does not work | posts 🔴 "did not work", escalates |
| 7 | Same fault twice in an hour | second time refuses, escalates |
| 8 | Pipeline outside the namespace | diagnoses fully, refuses to act |
| 9 | Model returns malformed output | falls back to tier 2, never guesses |
| 10 | Slack unreachable | agent continues, message printed locally |

Tests 2, 4, 7, 8 and 9 are the ones that demonstrate judgement. Tests 1 and
10 demonstrate it does not become a nuisance. Prioritise accordingly.

---

## 9. Non-goals for this build

Explicitly out of scope. Mention as roadmap, do not build:

- **UC2 schema-drift agent** — pre-flight schema diffing and DDL repair
- **Writing to the SOURCE system** — read-only, always human review (§5 zone 3)
- **TQL patch generation** (tier 2 diffs) — tier 2 posts a described change
  for a human to apply manually
- **Interactive Slack buttons** — needs a public callback URL
- **Multi-agent orchestration** — a single graph is enough today; note the
  specialist-agent design as Phase 2
- **Reading exception-store rows** — the tool reports *whether* rows were
  rejected; reading the payloads needs a `SELECT` that may not be exposed

---

## 10. Repository layout

```
striim-sre-agent/
├── PLAN.md                 this file
├── FINDINGS.md             the nine Striim observations
├── README.md               setup + how to run the demo
├── requirements.txt
├── setup.example.bat       env var template (no real secrets)
├── agent/
│   ├── __init__.py
│   ├── graph.py            LangGraph definition
│   ├── nodes.py            node implementations
│   ├── state.py            IncidentState
│   ├── prompts.py          system prompt + output schema
│   ├── striim_tools.py     read-only tools
│   ├── striim_actions.py   write path (namespace-locked)
│   ├── slack_io.py         threads, emoji, inbound commands
│   └── cli.py
├── legacy/                 the working prototype — reference, do not ship
└── tests/
    ├── fixtures/           captured health records for offline testing
    └── test_acceptance.py
```

**Secrets:** never in code. `STRIIM_USER`, `STRIIM_PASS`,
`ANTHROPIC_API_KEY`, `SLACK_BOT_TOKEN` from the environment. Ship
`setup.example.bat` with placeholders.

**Offline fixtures matter.** Capture real health records for each scenario
so the graph can be tested without breaking a live pipeline every time. The
prototype has several already.

---

## 11. Build order

Ship something working at each step. If time runs out, stop where you are —
a smaller thing that works beats a larger thing that does not.

1. Port `striim_tools.py` and `striim_actions.py` unchanged; verify each
   against the live instance
2. LangGraph skeleton: `WATCH → TRIAGE → DIAGNOSE → ESCALATE`, Slack outbound
3. Add `EVIDENCE` wait and `DECIDE` guardrails
4. Add `ACT` and `VERIFY`
5. Multi-pipeline monitoring
6. Slack inbound control via Socket Mode — `monitor` / `unmonitor` / `status`
7. Acceptance tests + demo dry run

---

## 12. The story the demo tells

For narrative, not implementation — but it should shape the ordering.

> Striim reliably tells you what it did. Nothing tells you what is actually
> in the target.
>
> At 3am a pipeline halts. The alert says *what*, never *why*. Someone gets
> up, connects, runs commands for twenty minutes, and often finds nothing was
> wrong at all. When something *is* wrong, the obvious fix — restart it — can
> silently duplicate every row in the target, and no alert will ever fire
> about that.
>
> This agent closes the gap between the alert and the fix. It diagnoses in
> minutes, repairs what it safely can, and — more importantly — knows when
> not to.
