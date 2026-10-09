# TASK 5 — The remaining fault classes

Eight break tests are being run against the live pipeline `sarah_moolai.sarah1_CDC`
(Postgres CDC, Neon, recovery enabled, `STRIIM_LIVE=1`).

Three are handled: **missing column** repairs end to end, **missing table**
recreates and backfills, **transient outage** resumes.

This document covers the rest. Each section states the fault, what the agent
does today, what it must do, and the acceptance test.

**The recurring bug across all of them is the same one:** a check that
succeeds is being read as "this end is healthy". Table exists ≠ schema
matches. Host answers ≠ credentials work. Status RUNNING ≠ data moving.

---

## 1. Type mismatch — widening

**Break**

```sql
ALTER TABLE "target" ALTER COLUMN "name" TYPE varchar(3);
INSERT INTO "source" ("name","city","age") VALUES ('ddddddddd','Chennai',30);
```

**Today.** Diagnosis is excellent — *"value too long for character
varying(3)' inserting id=345 name='ddddddddd'"*, high confidence, target
zone. It does not execute, and its own note says why: *"couldn't check: exact
declared length of target.name column (shown only as varchar(3) in error)."*

**Required**

- `db_describe_table` must return `data_type`, `character_maximum_length`,
  `numeric_precision`, `numeric_scale` from `information_schema.columns` for
  both ends. Reading a length out of an error string is the derivation the
  rules forbid, which is what blocked execution.
- `db_compare_schema` reports **type mismatch** as a first-class difference,
  not only missing and extra columns.
- **Widening joins the executable set**: `varchar(n)→text`,
  `varchar(n)→varchar(m>n)`, `int→bigint`, `numeric` precision increase.
  Nothing is lost. Narrowing stays destructive — capture first.

**Test.** Source `text`, target `varchar(3)`, `STRIIM_LIVE=1` → mismatch
measured from `information_schema`, not from the error → widening executed →
resumed → verified.

---

## 2. Constraint the source violates

**Break**

```sql
ALTER TABLE "target" ADD CONSTRAINT age_check CHECK (age < 40);
INSERT INTO "source" ("name","city","age") VALUES ('Old','Delhi',99);
```

**The danger.** The row lands in the exception store and **the pipeline keeps
running**. Every status check passes. This is finding §6 — rejected rows
dropped silently while the platform reports healthy.

**Required**

- **Rejections while RUNNING are a fault.** Do not report a pipeline healthy
  when `get_rejected_events` shows `has_ever_rejected` with a recent
  `seconds_since_last_rejection`. Say what is being lost.
- Compare **constraints**, not just columns and types: check constraints, NOT
  NULL, unique, foreign keys. A constraint present on the target and absent
  on the source is a rejection source.
- Classify: **target zone**, and the repair is dropping the constraint —
  destructive, so capture the constraint definition first so it can be
  restored.
- The incident must state **how many rows have been refused** and that they
  are not in the target.

**Test.** Constraint added to target, source row violates it, pipeline
RUNNING, exception store non-empty → incident raised, not "healthy" →
constraint named → row count stated.

---

## 3. Replication slot dropped — source zone

**Break**

```sql
-- stop the app first; an active slot cannot be dropped
SELECT pg_drop_replication_slot('striim_slot');
```

**Required**

- `db_replication_slot_status` already exists. When the slot is **absent**,
  that is categorically different from inactive: the stored position is gone
  and cannot be recovered.
- **Refuse to restart.** Restarting reads from wherever Striim decides to
  begin — most likely the start — and duplicates everything already
  delivered.
- **Never write to the source.** This is the zone-3 case.
- The escalation must be specific. Not "check the source system" but:

  > The replication slot `striim_slot` no longer exists on
  > `ep-…neon.tech:5432/neondb`, so there is no position to resume from.
  > Restarting would re-read from the beginning and duplicate the 1,204 rows
  > already in the target.
  >
  > Run this on the source, then start a fresh initial load:
  >
  >     SELECT pg_create_logical_replication_slot('striim_slot','wal2json');

- Note the plugin must be `wal2json` (finding §22) — a slot recreated with
  `pgoutput` fails only at runtime.

**Test.** Slot absent → tier 3 → slot named → creation statement includes
`wal2json` → no write reaches the source connection.

---

## 4. Permission revoked on the target

**Break**

```sql
REVOKE INSERT ON "target" FROM neondb_owner;
```

**This is the sharpest test in the set.** The host answers. The table exists.
The schema matches. Every check the agent runs passes — and writes still
fail. Expect it to conclude "the target is fine, must be the source", which
is the elimination error again.

**Required**

- Add `db_check_privileges(which, table)` — query
  `information_schema.role_table_grants` (or `has_table_privilege`) for
  SELECT / INSERT / UPDATE / DELETE on the tables the pipeline uses.
- Gather it in `EVIDENCE` whenever a target is implicated, alongside the
  schema comparison.
- **`db_probe` succeeding is not evidence the target is usable.** The probe's
  own note already says the credentials may not work; the evidence chain must
  act on that rather than reading a successful connection as a clean target.
- Missing privilege is **source-of-truth for the diagnosis** and the repair is
  a `GRANT` — which the agent's own credentials may not be permitted to issue.
  Propose it, named exactly:

      GRANT INSERT ON "public"."target" TO neondb_owner;

**Test.** Probe succeeds, table exists, schema matches, INSERT privilege
absent → diagnosis reaches *permissions*, not availability, and never places
the fault in the source.

---

## 5. Extra NOT NULL column the source cannot supply

**Break**

```sql
ALTER TABLE "target" ADD COLUMN "region" text NOT NULL;
INSERT INTO "source" ("name","city","age") VALUES ('X','Y',20);
```

**Why it matters.** Every case so far is "target missing something". This is
the reverse. If `db_compare_schema` only walks source→target, it comes back
clean and the agent is blind.

**Required**

- The diff must be **bidirectional**: columns in source not in target, **and
  columns in target not in source**, with nullability and defaults for both.
- A target column that is NOT NULL, has no default, and has no source
  counterpart **rejects every write**. That is the diagnosis.
- Two repairs, both destructive-ish, so both capture first:
  - `ALTER TABLE ... ALTER COLUMN "region" DROP NOT NULL` — preferred, loses
    nothing
  - `ALTER TABLE ... DROP COLUMN "region"` — only if the column is empty

  Prefer dropping the constraint over dropping the column. State which was
  chosen and why.

**Test.** Target has an extra NOT NULL column with no default and no source
counterpart → diff reports it → `DROP NOT NULL` preferred over `DROP COLUMN`
→ executed → resumed.

---

## 6. Silent stall — RUNNING with a frozen position

**Observed live**, no break needed. `sarah1_CDC` reported RUNNING, moved
nothing for 34 minutes, slot `active = false`, and **ten consecutive
checkpoints recorded the identical LSN `0/1D5F690` while their timestamps
advanced.**

The agent diagnosed it correctly and then declined:

> *considered wait_and_resume, but RUNNING is not a fault state*

**Required**

- A **derived fault condition** independent of the status word. A pipeline is
  stalled when any of these hold:
  - checkpoint position unchanged across N consecutive checkpoints while
    timestamps advance
  - replication slot `active = false` while the app claims RUNNING
  - both ends silent beyond what the connector profile calls normal
- A stall counts as a fault for tiering and for skill preconditions.
  `wait_and_resume` must be able to fire — a stalled CDC reader reattaching
  to its slot is the textbook safe restart.
- Fix the contradiction in the headline: it currently reads *"stopped, not
  safe to restart"* while the status is RUNNING.

**Test.** Ten checkpoints at one LSN with advancing timestamps, slot
inactive, status RUNNING → treated as a fault → `wait_and_resume` fires →
resumed → verified by the LSN advancing, not by the status word.

---

## 7. Cross-cutting — every declined skill must be checkable

Seen twice now: *"considered X, but there is no schema difference"* printed
in the same message as *"schema diff shows source has 'age', target does
not"*. Root cause was `applies()` and the verdict reading different state.

**Required**

- Any *"considered X but declined"* line must name the **specific evidence
  that was missing or negative**, and that evidence must be present in the
  incident so a human can check the reasoning.
- If a skill declines because evidence was **never gathered**, say that —
  *"never compared"* is different from *"compared and matched"*, and the
  distinction is the single most repeated bug in this project.

---

## Priority

If not all fit:

1. **§6 silent stall** — the fault nothing else on the platform can see, and
   the strongest thing in the demo
2. **§4 permission** — will expose the elimination error again
3. **§1 widening** — nearly working, blocked only on reading types properly
4. **§2 constraint** — finding §6 made actionable
5. §5 reverse diff
6. §3 slot escalation

Everything here is one live pipeline and takes under five minutes per test to
reproduce.
