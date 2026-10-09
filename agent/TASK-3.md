# TASK 3 — Repair skills

**Context:** the agent diagnoses well and refuses well. What it cannot yet do
is *fix* anything beyond a plain restart. File pipelines are the toy case;
what Striim actually needs is repair for the failure modes their own Smart
Alerts enumerate — backpressure, checkpoint stalls, repeated termination,
rejected rows.

This task adds a **skill layer**: a small set of named, precondition-guarded
repairs the agent can propose and, where safe, apply.

---

## Priority 0 — Two fixes from live testing

Do these first. Both are small and both are actively degrading the demo.

### 0.1 `lastModifiedTime` is generating false causality

Observed live:

> "Terminated shortly after config edit. App terminated; definition was
> edited about 16 minutes before fault."

There was no relationship. The field is *when the application definition was
last edited* (`PLAN.md` §6.5) and the app had just been redeployed. The model
built a plausible causal story out of a coincidence — the exact failure mode
this design exists to prevent.

**Fix:** either withhold the field from the model entirely, or rename it to
`definition_edited_at` and attach an explicit warning that it has no
relationship to fault timing and must not be used to build a timeline.

Add a test: given a record where the definition was edited shortly before a
fault, the verdict must not cite the edit as a cause.

### 0.2 The evidence wait is three minutes of dead air

`issuesList` is empty for all 577 apps on this instance. The wait
(4 × 45s) therefore almost always expires having found nothing, and it is the
largest single component of a ~7 minute detection-to-incident time.

**Fix:** default to 2 × 20s. Keep the flag for instances that do publish
error text. Log the outcome distinctly — "no error text on this cluster"
reads better than four identical waiting lines.

---

## 1. The skill interface

A skill is not a prompt instruction. It is a declared capability with
machine-checkable preconditions.

```python
@dataclass
class Skill:
    name: str
    zone: Literal["pipeline", "target", "source"]
    reversible: bool
    description: str            # what the model sees

    def applies(self, state: IncidentState) -> tuple[bool, str]:
        """Is the evidence for this skill actually present?
        Returns (yes/no, why). Runs in CODE, never in the prompt."""

    def plan(self, state: IncidentState) -> RepairPlan:
        """What exactly would be done - statements, counts, expected effect.
        Always produced, even when the skill will not be executed."""

    def execute(self, state: IncidentState, plan: RepairPlan) -> Result:
        """Only reached after applies() passed AND the tier allows it."""
```

**The rule, unchanged:** the model may *propose* a skill. `applies()` decides
whether the evidence supports it. The tier decides whether it may run. A
confident model gets no extra rope.

`plan()` runs even for skills that will not execute — the plan is what gets
posted for a human. An escalation that says "here is exactly what I would
have done" is worth far more than one that says "needs a human".

---

## 2. The skills

Build in this order. 1–3 are the sweet spot: pipeline zone, reversible, and
they cover what Striim's own alerts fire on.

### Skill 1 — `wait_and_resume`

| | |
|---|---|
| Zone | pipeline |
| Reversible | yes |
| Tier | 1 when preconditions hold |

**Fires when:** the fault is HALT (external cause), the dependency now
responds, and there is a usable checkpoint — or the source is one where a
cold start does not duplicate.

**Preconditions (all must hold):**
- status is a fault state
- the failed component is identified
- the dependency responds now (see §3 — reachability probe)
- `has_usable_checkpoint` is true **and** the recorded position is plausible
  (§6.4 — a checkpoint can exist and be nonsense)
- restart budget not spent, fingerprint not already attempted

**Execute:** `RESUME`, then verify per `PLAN.md` §4.3.

### Skill 2 — `tune_batch_policy`

| | |
|---|---|
| Zone | pipeline |
| Reversible | yes |
| Tier | 2 — propose, human approves |

**Fires when:** backpressure is sustained, the target is reachable and
writing, but throughput is far below the source rate. This is the
`Application_Backpressured` alert.

**Preconditions:**
- source rate meaningfully exceeds target rate over two consecutive polls
- target is not erroring — it is keeping up badly, not failing
- current batch/flush settings retrieved via `describe_component`

**Plan:** the specific property change, current value → proposed value, and
the reasoning. Do not guess: read the current policy first.

### Skill 3 — `propose_ignorable_exception`

| | |
|---|---|
| Zone | pipeline |
| Reversible | yes |
| Tier | 2 — always. Never tier 1. |

**This is the highest-value skill in the list.** An application that
terminates on every occurrence of the same recoverable exception is a support
ticket. Identifying the exception class, confirming it is isolated rather
than systemic, and proposing it be ignored is a support engineer's afternoon.

**Fires when:** the same exception class recurs across restarts, and the
exception store shows the affected rows are a small fraction of throughput.

**Preconditions:**
- the same fingerprint has occurred ≥ 2 times
- exception store exists and has received rows
- affected row count is small relative to total (make the ratio a config
  value; default 1%)

**Plan:** name the exception class, the row count, the proportion, and the
exact configuration change. **State plainly that ignoring it means those rows
will be skipped**, and say how many.

**Never tier 1.** This changes data semantics. A human decides whether losing
those rows is acceptable — that is not a judgement an agent should make.

### Skill 4 — `recreate_missing_target_object`

| | |
|---|---|
| Zone | target |
| Reversible | no |
| Tier | 2 — propose only, DDL is never executed |

**Fires when:** the writer reports a missing table or column.

**Plan:** the `CREATE`/`ALTER` statement, derived from the source schema
where available. Written out for a human to run.

DDL stays propose-only — `PLAN.md` §5 rule 4 requires a `WHERE` clause and
DDL cannot have one. This narrowing was already agreed and stands.

### Skill 5 — `dedupe_target_rows`

| | |
|---|---|
| Zone | target |
| Reversible | no |
| Tier | 2 by default; 1 only under the row threshold |

**This is the skill that closes the loop on our own central finding.** We
demonstrated that a re-read silently duplicates every row and no alert fires.
This is the fix.

**Preconditions:**
- a re-read is confirmed (source position reset, or restart-after-replacement
  observed)
- the target has a primary key or unique constraint — **without one there is
  no safe dedupe and the skill must decline**
- the duplicate count has been measured, not estimated

**Plan:** the `DELETE` with its `WHERE` clause, and the exact row count from
running the equivalent `SELECT COUNT(*)`. Per `PLAN.md` §5, the count must be
derived from the same predicate as the statement.

**Note for the writeup:** append-only targets — files, S3, lakes in append
mode — cannot deduplicate by construction. There is no key to match on. The
skill should say so explicitly rather than failing obscurely, because that
sentence is itself a finding worth showing.

### Skill 6 — `clear_partial_batch`

| | |
|---|---|
| Zone | target |
| Reversible | no |
| Tier | 2 |

**Fires when:** a resume would collide with rows from a half-written batch.

Same discipline as skill 5: bounded `WHERE`, measured count, statement shown
before execution.

### Skill 7 — `escalate_source_problem`

| | |
|---|---|
| Zone | source |
| Reversible | n/a — nothing is done |
| Tier | 3 always |

**Fires when:** credentials rejected, host unreachable, replication slot
purged past the stored position, source object missing.

**This skill never acts.** Its output is a precisely worded escalation: the
exact object, the exact host, the exact position, and what a person must do.

Its value is in the specificity. "Check the source system" is useless.
"Replication slot `striim_slot` on `db.example.com` has been purged past LSN
0/3A2B1C0 — the pipeline cannot resume and needs a fresh initial load" is
actionable at 3am.

---

## 3. Reachability probe

Skills 1 and 7 both need to answer: **is the dependency actually back?**
Nothing today answers this — it is the single largest gap.

Add `check_dependency(component)`:

- read the component's configured endpoint via `describe_component`
- for a **file** source: does the configured path now exist and is it readable
- for a **database**: open a connection with the configured credentials, run
  `SELECT 1`, close
- for an **API/SaaS** source: a cheap authenticated call if one exists
- for anything else: return an explicit "cannot probe this connector type"

**Rules:**
- read-only, always — never a write, never a schema query
- short timeout (5s), never blocks the monitoring loop
- **"cannot probe" is a valid answer** and must be distinguishable from
  "probed and it failed". Same discipline as everywhere else in this project:
  an empty result is not a negative finding.

Credentials come from the environment, not from Striim's stored config —
we do not extract secrets from the platform.

---

## 4. How skills reach the model

Add to the verdict schema:

```json
"suggested_skill": "wait_and_resume" | "tune_batch_policy" | ... | null
```

The system prompt lists the skills with their descriptions and zones. The
model picks one or none.

Then, in code:

1. `applies()` on the suggested skill — if the evidence is not there, the
   skill is dropped and the reason recorded
2. tier rules decide propose vs execute
3. `plan()` always runs; the plan goes in the incident either way
4. `execute()` only if both gates passed

**If the model suggests a skill whose preconditions fail, that is worth
saying in the incident** — "considered dedupe, but the target has no primary
key" tells a human something useful about their pipeline.

---

## 5. Acceptance tests

| # | Scenario | Expected |
|---|---|---|
| 1 | HALT, dependency now reachable, valid checkpoint | `wait_and_resume` executes, verifies |
| 2 | HALT, dependency still unreachable | skill declines, escalates with the endpoint named |
| 3 | HALT, checkpoint present but position implausible | declines — cites §6.4, not the checkpoint flag |
| 4 | Same fingerprint twice, few rejected rows | `propose_ignorable_exception` at tier 2, never 1 |
| 5 | Same fingerprint, rejected rows are 40% of throughput | declines — systemic, not isolated |
| 6 | Duplicates confirmed, target has a key | `dedupe_target_rows` plans with a real count |
| 7 | Duplicates confirmed, append-only target | declines, explains why dedupe is impossible |
| 8 | Missing target column | DDL proposed, never executed |
| 9 | Credentials rejected | tier 3, names the host and the user |
| 10 | Model suggests a skill whose preconditions fail | dropped, reason stated in the incident |
| 11 | Model suggests a target skill on a source fault | refused by zone check |

Tests 3, 5, 7, 10 and 11 are the ones that demonstrate judgement. Prioritise.

---

## 6. Not in scope

- Executing DDL
- Writing to the source, ever
- Schema drift / UC2
- Any skill that cannot state its preconditions in code
