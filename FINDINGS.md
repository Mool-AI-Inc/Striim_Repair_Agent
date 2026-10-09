# What Striim actually does

Observations from breaking a real Striim instance repeatedly over several
days, then checking every one of them against a full capture of the shared
dev cluster — 576 pipelines, 228 of them in HALT. Several contradict the
documentation. Every one is load-bearing somewhere in this repo, and each
says where, so that if a finding turns out to be wrong you can find the
code that assumes it.

**Items 1–10 are behaviours.** Items 11–16 are *shapes*: field names and
structures that are not what they look like. Those six were found by
writing the tool layer against invented fixtures and then pointing it at a
real capture, at which point six things broke at once. If you take one
thing from this file, take that: a passing test against a fixture you wrote
yourself proves your logic, not your assumptions.

Read this before writing any integration code against Striim.

---

## 1. The console endpoint's auth is not what the docs say

`/api/v2/tungsten` wants a **plain-text body** and an
`Authorization: token <value>` header.

| What you send | What you get |
|---|---|
| JSON body | `415 Unsupported Media Type` |
| `STRIIM-TOKEN` header | `401` |
| plain text + `Authorization: token …` | works |

```python
requests.post(f"{STRIIM_URL}/api/v2/tungsten",
              headers={"Authorization": f"token {token}",
                       "Content-Type": "text/plain"},
              data="DESCRIBE ns.App_FileReaderSource;")
```

Both failure modes look like "the command didn't work" rather than "you
called it wrong", which is what made this expensive.

*Used in:* `striim_tools.console`, `striim_actions.run_command`

---

## 2. `/health` lags roughly two minutes

It is a cached cluster snapshot, not a live view. Two consequences:

- Detection cannot be faster than the lag. Polling every ten seconds buys
  nothing but load.
- **Verification must outwait it.** Read back too soon and you get the
  pre-action world, and a fix that worked is reported as a failure. Use
  ≥ 210 seconds, not 120 — "roughly two minutes" is not a guarantee, and
  being early is the expensive direction to be wrong.

*Used in:* `config.VERIFY_WAIT_SECONDS`, `striim_actions.verify`

---

## 3. Error text is published separately from the status change

And later. Poll the instant a fault appears and you get a status with no
reason attached, which produces a confident diagnosis of nothing.

Wait and re-check — 45 seconds, four times — before spending a model call.

But only if the text is still coming. A fault that broke months ago is
never going to publish one, and "check again shortly" is worse advice than
"no cause was ever recorded". The tool works out which case it is from the
fault's age.

*Used in:* `nodes.evidence`, `striim_tools.get_errors`

---

## 4. A checkpoint can exist and be unusable

Observed: recovery enabled, checkpoints recorded with real timestamps,
`has_usable_checkpoint: true` — and a recorded read offset of **170,969
bytes on a file of about 1KB**.

The offset had accumulated across successive replacements of the file and
no longer referred to the file that was there. Striim cannot seek to it, so
it re-reads from the start and duplicates everything into the target.

**Therefore `has_usable_checkpoint == true` is not sufficient to conclude a
restart is safe.**

There is no arithmetic that settles this. Striim reports the position but
not the size of the file it points into, so 170,969 is unremarkable on a
2MB file and catastrophic on a 1KB one — any byte threshold you pick will
pass this case or fail a real one.

What settles it is *which component failed*: if the source itself is what
broke, the thing the position points into is the thing that is missing, so
the position is worthless. If the source is fine and the fault is
downstream, the reader is still sitting where it was.

And when nothing says which component failed — the normal case here, see
§12 — the answer is to refuse. Not out of caution: there is genuinely no
evidence either way, and the cost of being wrong is a silently doubled
table that no alert will ever fire about.

*Used in:* `nodes.restart_risk`, `striim_tools._plausible_offset`

---

## 5. `lastModifiedTime` is not "last activity"

It is when the application *definition* was last edited. Using it to age a
fault invents a fault age out of nothing — an app edited ten days ago and
broken four minutes ago will tell you the fault is ten days old.

Age faults from the state-change timestamps instead.

**And a warning label is not enough.** This field was originally exposed as
`definition_last_edited_seconds_ago`, with a comment in the code and a note
in the tool output saying what it was. Observed live anyway:

> "Terminated shortly after config edit. App terminated; definition was
> edited about 16 minutes before fault."

There was no relationship. The app had just been redeployed. A plausible
causal story assembled out of a coincidence — the exact failure this whole
design exists to prevent, produced by a correctly-labelled field.

The lesson generalises: **a number next to a fault is an invitation to
subtract.** Naming it carefully does not withdraw the invitation. So the
relative age is gone entirely, the field is an absolute wall-clock string
called `definition_edited_at`, and it carries its warning as a sibling key
the model reads next to the value rather than as a comment it never sees.

Remove the arithmetic, not just the label.

*Used in:* `striim_tools.get_status`, `striim_tools._when`,
`striim_tools.fault_age`, and a prohibition in the system prompt

---

## 6. Zero throughput means different things in different states

`DEPLOYED` (never started), `COMPLETED` (finished normally) and a stalled
`RUNNING` all show zero. The status word alone cannot tell them apart.

Tools must state what each state *means*, not just report it — otherwise
the model infers, and it infers plausibly and wrongly.

*Used in:* `striim_tools.STATUS_MEANING`

---

## 7. The deliberate-shutdown test only applies one way

A clean shutdown passes through `STOPPING`, so `STOPPING` in the recent
path means a human did it.

But `STOPPED → RUNNING` is a *start*, and starts are always requested — by
a person or by auto-resume. The absence of `STOPPING` there means nothing
at all. Applying the test to a start is a category error.

*Used in:* `striim_tools.get_state_history`

---

## 8. The state-change list is a short rolling window

Entries age out. An empty list means **"I have no data"**, not "it was not
deliberate".

Reading silence as "nobody asked for this" is exactly how an agent talks
itself into restarting something a human switched off on purpose.

*Used in:* `striim_tools.get_state_history` (returns
`looks_deliberate: None`, never `False`, when the window is empty)

---

## 9. Striim only watches the reading side

Delete the **target** file and there is no alert, no discarded events, and
no status change. The writer simply creates a new one and carries on.

This is a genuine blind spot, and worth demonstrating: it is the clearest
illustration of the gap between "Striim says it worked" and "the data is
there".

*Used in:* nothing yet — the agent cannot see this either, because Striim
does not report it. Noted here because it is the honest limit of a
control-plane agent, and it is where a data-plane check would go next.

---

## 10. In/out counters measure work done, not data present

Observed 9,900 in / 9,900 out reconciling perfectly while the target file
held about 1,900 rows — deletions had happened outside Striim's view.

**A matched in/out count is not proof of correctness.** It proves the
pipeline moved what it read, which is a different claim.

*Used in:* `striim_actions.verify` — which is why verification checks that
the target wrote *since the action* and that the checkpoint *advanced*,
rather than reconciling totals.

---

---

# Shapes — what the fields actually are

All six of these were found in one afternoon, by pointing a tool layer that
passed 40 tests at a real capture for the first time.

## 11. `checkpointInformation` is a list of TIMES, not a map of positions

It reads like a per-component map of read positions. It is neither:

```json
[{"firstRecordedCheckpoint":  {"checkpointTime": "2026-08-03 19:56:50",
                               "checkpointDuration": "139ms",
                               "checkpointType": "normal"}},
 {"lastTwoRecordedCheckpoints": [{...}, {...}]}]
```

A list of two entries, carrying **when** checkpoints happened and never
**where the reader got to**. So `get_checkpoints` can tell you recovery is
on and has been used; it can never tell you a restart is safe. The position
lives in `describe_component`, on a different endpoint.

The whole structure also appears with every value as the literal string
`"N/A"` — recovery enabled, nothing ever written. 131 of the cluster's HALT
apps look like that, and reading only the outer structure reports "recovery
is on, safe to resume" on every one of them.

*Used in:* `striim_tools.get_checkpoints`, `_walk_checkpoints`

---

## 12. `issuesList` is empty. For every application. Always.

228 applications in HALT, and `issuesList` had **length zero**. Not "no
entry for our app" — no entries at all, for anyone.

This is the finding that moved the agent's centre of gravity. `get_errors`
was written expecting to be the primary evidence source, and on this
deployment it returns nothing every time. The cause has to come from
`describe_component` — what the component is configured to read, and where
it got to — which is why an incident can still name `AI.csv` with no error
message anywhere in the system.

It also changes what "no error text" *means*. "No error was recorded for
Trial5" implies Trial5 is unusual. It is not: nothing is being recorded for
anything, which is a fact about the cluster and not about the pipeline. The
tool says so, because the difference sends a human looking in a different
place.

It also has a cost in wall-clock time. The agent waited 4 × 45s for text
that was never coming — three minutes of dead air on every single incident,
and the largest component of a seven-minute detection-to-incident time. It
now checks whether the cluster publishes *anything* and skips the wait
entirely when it does not; where text really can arrive late, the default
is 2 × 20s.

⚠️ **The one contract still unverified.** Because the list is always empty,
the `{"fqName": …, "issue": …}` shape inside it is inherited from the
prototype and has never been re-checked against a populated response. If
error text ever does appear, check those two key names first.

*Used in:* `striim_tools.get_errors` (`cluster_publishes_errors`)

---

## 13. `stateChangeList` holds server events, not application ones

Filtering it by `fqName` was already right, but the reason matters: in a
capture with ten entries, **all ten** were `Global.striim-2` and
`Global.adhocquery…` — server and ad-hoc-query transitions. Zero
application transitions, for any of the 576 apps.

Entries also carry a `type` of `APPLICATION` or `SERVER`.

So the deliberate-shutdown test in §7 is real but rarely has data to work
with, and §8 matters more than it first appeared: an empty list is the
normal case, and it means **"I have no data"**.

*Used in:* `striim_tools.get_state_history`

---

## 14. Component names do not follow the documented convention

The convention is `<App>_<Adapter><Role>`. Observed in one capture:

| Real name | What it breaks |
|---|---|
| `sarah_moolai.Trial5_FileReaderSource` | the documented shape — fine |
| `sarah_moolai.File_Trial5_Target` | app name in the MIDDLE, no "Writer" |
| `MANIMARAN22.OracleToFabricDWH_CDC_Target` | no adapter word at all |
| `bharath_qatest_c4aapp.Srcoracle19cpostgres12sanity` | app is `Apporacle…`; no shared name at all |
| `harrybwatson.Vercel_to_Azure_SQL_CDC_PostgresCDC` | adapter is `PostgresCDC`, not `PostgresReader` |

Two consequences. First, matching components to an app needs a fallback
beyond the prefix — but one that matches **whole name-parts**, because on a
576-pipeline cluster a substring test hands `Trial5` the components of
`Trial50`. Second, some apps genuinely cannot be matched at all, and the
honest answer there is "I cannot see them", not "it has none".

Connector profiles have to cope with the same mess, which is why they
resolve full adapter names first, then fall back to product words — and
only ever within the component's own role. `…OracleToFabricDWH_CDC_Target`
contains "oracle" and is a *warehouse writer*; reading it as a CDC reader
would be confidently wrong.

*Used in:* `striim_tools._components_for`, `profiles.resolve_adapter`

---

## 15. Byte offsets are strings with thousands separators

`"Offset Begin": "170,969"`, not `170969`. `int()` throws on it.

Silent damage: the offset check returned "this is not a number, so it
cannot be checked", which reads like a tool limitation rather than a parse
bug, and the safety logic downstream quietly stopped having an offset to
reason about.

The same block also carries `Record Length` and a `Creation Time`, and
appears twice — as `Source Current Position` and `Source Restart Position`.

*Used in:* `striim_tools._bytes`, `describe_component`

---

## 16. Two more that look like numbers and are not

**`lagEnd2End` is a nested statistics blob**, or `null`:

```json
{"data": [{"recentStats": {"latest": 0.08, "average": 0.08, ...},
           "type": "Idle", "lee": 0.08}]}
```

Handing that to the model raw wastes a third of the context window on one
field. `type: "Idle"` is the useful part.

**A timestamp of `0` means "never"**, and is different from a missing
field, which means "not reported". Collapsing both to `None` loses a real
finding: a CDC source with `lastEventTime: 0` has never read a single
change — which for change capture is the whole story.

Property keys also come back **lower-cased** from `DESCRIBE` — `directory`,
`wildcard`, `filename` — whatever case they were written in.

*Used in:* `striim_tools._lag`, `_has_ever`, `describe_component`

---

## 17. A console `SELECT` is not a read

It compiles into a **transient application in the `Global` namespace**.
Captured, in a health record taken while nothing but a console query was
running:

```
stateChangeList:  Global.adhocquery1f191219d7402c1b1872af9bf95d7f2
                  type APPLICATION, currentStatus CREATED
```

Two of them, four seconds apart. None of them appear in `appHealthMap`, so
they are short-lived — but they are real objects, created **outside the
namespace this agent is otherwise locked to**.

That collided with two claims the code was making. `striim_tools.py`'s
header said "nothing in this file can change anything on the cluster",
which was false while `SELECT` was in `READ_ONLY_VERBS`. And "writes only
inside the configured namespace" was false for the same reason. Nobody had
counted this as a write.

**Therefore:** `SELECT` is not in `READ_ONLY_VERBS`, `console_read` refuses
it by name and says why, and there is no general SQL tool. The one place
the agent issues a `SELECT` is `get_rejected_rows`, which builds the
statement itself against a store name it has checked against
`waStoreHealthMap`, caps the row limit in code, and runs once per incident.
The model supplies an application name and nothing else.

`PREVIEW` is likely to behave the same way and has not been observed.

*Used in:* `striim_tools.READ_ONLY_VERBS`, `console_read`,
`get_rejected_rows`

---

## 18. A console `SELECT` returns a handle, not rows — and the rows are not retrievable

**Probed directly against the live cluster.** Running a SELECT through
`/api/v2/tungsten` returns:

```
adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 01f19293-c455-ef51-b6f6-22ca1b2e804a
```

Four fields: the ad-hoc query application Striim compiled the statement
into, the literal word `QUERY`, an index, and a UUID. The application name
matches the `Global.adhocquery<hex>` entries in `/health`'s
`stateChangeList`, which is what confirms §17.

`tools/probe_adhoc.py` then tried all three plausible retrieval routes — a
results endpoint keyed by the UUID or the query name across ~40 paths and
both auth styles, the query application appearing in `/health` with a
readable store, and a follow-up console command against the query name.
**None returned rows.**

So the rows are not reachable over REST, and that is a settled negative
rather than an open question.

**What follows from it:**

- `get_rejected_events` reports the rejection, the count, the age and the
  **exact statement** a human can run. It costs nothing to produce and it
  is everything that can honestly be offered.
- `get_rejected_rows` is off the automatic evidence path. Calling it per
  incident would create a transient `Global.adhocquery*` application and
  come back with the same statement. It stays available, and will start
  working for free if a future Striim version answers differently.
- The handle is parsed rather than treated as opaque, so wiring up a
  results endpoint later is one function and not a fresh investigation.

This is still a large improvement on where it started. "Row `id=9` violates
`target_pkey`, run this to see the rest" is a diagnosis; "inspect the
exception store" was an instruction to go and do the agent's job.

*Used in:* `striim_tools.parse_query_handle`, `get_rejected_events`,
`tools/probe_adhoc.py`

---

## 19. Response shapes that are still unverified

Every console response ever captured from `/api/v2/tungsten` is a
`DESCRIBE`, and they come back as structured JSON:

```json
[{"command": "...", "executionStatus": "Success",
  "output": [...], "responseCode": 200}]
```

TASK-4 §2 assumed `MON` returns human-formatted tables needing a
box-drawing parser. That is the *interactive Tungsten CLI*, a different
surface, and `MON` over REST has not been observed. Writing that parser now
would mean writing it against a fixture we invented — which is exactly what
§11–16 are a record of.

So `_parse_console` classifies what arrives — `structured`, `text`,
`error`, `unknown` — reports it for what it is, and in every unreadable
case hands back **the literal statement and where to run it**. `rows: []`
never means "I could not read them"; `shape` says which happened. The table
parser gets written when a real response needs one.

*Used in:* `striim_tools._classify_rows`, `_parse_console`

---

## The rule these add up to

Every one of these is a case where an empty, missing or plausible-looking
value meant something other than what it appeared to mean. So:

> Every tool must be able to say **"I don't know"** as something distinct
> from **"no"**.

Three of the five bugs found during prototyping were tools reporting
missing data as a negative finding, which walked the model into confident
wrong conclusions. An empty result is not evidence of absence.

And a second rule, from items 11–16:

> **Test the shape against a real capture, not against a fixture you
> wrote.**

Forty tests passed against invented fixtures. Six field names were wrong,
including two the entire safety argument rests on. The fix is not more
tests — it is one test per tool asserting its documented keys against a
record the cluster actually produced. That is
`tests_the_contract_*` in the acceptance suite.
