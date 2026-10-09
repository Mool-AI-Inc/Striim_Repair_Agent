"""
Repair skills
=============

A skill is not a prompt instruction. It is a declared capability with
machine-checkable preconditions:

    applies(state) -> (bool, why)   is the evidence for this ACTUALLY here?
    plan(state)    -> RepairPlan    exactly what would be done
    execute(...)   -> dict          only after applies() AND the tier agree

The rule is the same one the whole agent is built on, applied one level
down: **the model may propose a skill; the code decides whether the
evidence supports it, and the tier decides whether it may run.** A confident
model gets no extra rope. If the diagnosis suggests dedupe and the target
has no primary key, the skill declines - and it says so, because "considered
dedupe, but the target has no primary key" tells a human something real
about their pipeline.

plan() runs even for skills that will never execute. An escalation that
says "here is exactly what I would have done, and it would have touched 412
rows" is worth far more than one that says "needs a human". Most of these
skills are propose-only by design; the plan IS the product.

Three things no skill may ever do, enforced here and again in the layers
below: write to a source, execute DDL, or act outside the namespace.
"""

import re
from dataclasses import dataclass, field
from typing import Optional

from . import backfill
from . import config
from . import db
from . import ddl
from . import probes
from . import target_repair


@dataclass
class RepairPlan:
    """Exactly what a skill would do. Always produced, rarely executed."""

    skill: str
    zone: str
    summary: str                      # one line, for the incident
    effect: str                       # what changes, in plain words
    reversible: bool
    statements: list = field(default_factory=list)   # SQL / console, verbatim
    counts: dict = field(default_factory=dict)       # measured, not estimated
    warnings: list = field(default_factory=list)
    executable: bool = False
    reason: str = ""

    # When the agent cannot run something itself, THIS is what a human is
    # given: the finished statement, where to run it, what it was derived
    # from and how to undo it. Set it only when there really is a command -
    # a skill whose next step is "restore a file" leaves it empty, and the
    # message uses the plain-English action instead.
    command: str = ""
    run_on: str = ""
    derived_from: str = ""
    reversal: str = ""

    # True when the skill has already restarted the application itself, so
    # the resume step must not run on top of it.
    already_restarted: bool = False

    def as_dict(self):
        return {
            "skill": self.skill, "zone": self.zone, "summary": self.summary,
            "effect": self.effect, "reversible": self.reversible,
            "statements": list(self.statements), "counts": dict(self.counts),
            "warnings": list(self.warnings), "executable": self.executable,
            "reason": self.reason, "command": self.command,
            "run_on": self.run_on, "derived_from": self.derived_from,
            "reversal": self.reversal,
            "already_restarted": self.already_restarted,
        }


class Skill:
    """Base class. Subclasses override applies / plan / execute."""

    name = "skill"
    zone = "pipeline"
    reversible = True
    max_tier = 2            # the BEST tier this skill may ever reach
    description = ""

    def applies(self, state):
        """(yes/no, why). Runs in code, never in the prompt."""
        return False, "not implemented"

    def plan(self, state):
        raise NotImplementedError

    def execute(self, state, plan, services):
        raise NotImplementedError(f"{self.name} does not execute")

    # -------------------------------------------------------- helpers

    @staticmethod
    def _evidence(state):
        return state.get("evidence", {}) or {}

    @staticmethod
    def _source(state):
        return ((state.get("evidence", {}).get("flow") or {})
                .get("source") or {})

    @staticmethod
    def _target(state):
        return ((state.get("evidence", {}).get("flow") or {})
                .get("target") or {})


# =========================================================== 1. RESUME

class WaitAndResume(Skill):
    """The one skill that acts on its own, and only when everything lines up."""

    name = "wait_and_resume"
    zone = "pipeline"
    reversible = True
    max_tier = 1
    description = ("Resume a pipeline whose external cause has cleared. Use "
                   "when the fault was outside Striim - a target that went "
                   "away, a host that rebooted, a network blip - and the "
                   "thing it depends on is responding again. Not for faults "
                   "whose cause is still present, and not where a restart "
                   "would re-read data.")

    def applies(self, state):
        from .nodes import restart_risk       # local: nodes imports us back

        status = state.get("status")
        stalled = ((self._evidence(state).get("stall") or {}).get("stalled"))

        # A stall IS a fault, whatever the status word says - and it is the
        # textbook safe restart: a CDC reader that has lost its grip on the
        # slot reattaches to the position the slot still holds. Declining
        # this because "RUNNING is not a fault state" was the agent
        # diagnosing a stall correctly and then doing nothing about it.
        if status not in ("HALT", "TERMINATED", "CRASH") and not stalled:
            return False, (f"{status} is not a fault state and nothing "
                           f"suggests it has stalled")

        risk = restart_risk(self._evidence(state))
        if risk:
            return False, risk

        probe = self._evidence(state).get("dependency")
        if probe:
            if probe["reachable"] is False:
                return False, (f"the dependency is still down — "
                               f"{probe['detail']}")
            if probe["reachable"] is None:
                # Not a refusal on its own. The restart-risk check above
                # already covers the expensive failure, and refusing every
                # unprobeable connector would refuse almost all of them.
                pass

        if stalled:
            return True, ("it is stalled — reported running while moving "
                          "nothing, and a restart reattaches it to the "
                          "position its slot still holds")

        return True, "the fault is external and a restart cannot re-read"

    def plan(self, state):
        app = state["app"]
        probe = self._evidence(state).get("dependency")
        checkpoints = self._evidence(state).get("checkpoints") or {}

        warnings = []
        if probe and probe["reachable"] is None:
            warnings.append(f"Could not confirm the dependency is back: "
                            f"{probe['detail']}")
        if not checkpoints.get("has_usable_checkpoint"):
            warnings.append("No stored position — this is a cold start.")

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=f"resume {app.rsplit('.', 1)[-1]}",
            effect="The pipeline starts again from its stored position and "
                   "carries on. Nothing is re-read and nothing is skipped.",
            statements=[f"RESUME {app};"],
            counts={},
            warnings=warnings,
            # applies() has already established this is safe, and a RESUME
            # is bounded by the action layer's own gates. The tier still
            # decides whether it actually runs.
            executable=True,
            reason="the fault is external, the cause has cleared, and a "
                   "restart cannot re-read",
        )

    def execute(self, state, plan, services):
        from . import striim_actions as act
        return act.run_command(services.striim, state["app"], "RESUME",
                               fingerprint=state.get("fingerprint"),
                               ledger=services.ledger)


# ==================================================== 2. BATCH POLICY

# Striim writes these as "eventcount:100000,interval:30s".
BATCH_KEYS = ("flushpolicy", "uploadpolicy", "rolloverpolicy", "batchpolicy")


class TuneBatchPolicy(Skill):
    """The Application_Backpressured case: keeping up badly, not failing."""

    name = "tune_batch_policy"
    zone = "pipeline"
    reversible = True
    max_tier = 2
    description = ("Propose a smaller batch or shorter flush interval when a "
                   "pipeline is backpressured - the source is producing "
                   "faster than the target is writing, but the target is "
                   "healthy and is not rejecting anything. Use for sustained "
                   "backpressure, not for a fault.")

    def applies(self, state):
        source, target = self._source(state), self._target(state)

        source_rate = source.get("event_rate")
        target_rate = target.get("event_rate")
        if source_rate is None or target_rate is None:
            return False, "throughput is not reported for both ends"

        if source_rate <= 0:
            return False, "the source is not producing anything, so there is "\
                          "no backpressure to relieve"

        if target_rate >= source_rate * 0.8:
            return False, (f"the target is keeping up "
                           f"({target_rate:g} vs {source_rate:g} events/s) — "
                           f"this is not backpressure")

        rejected = self._evidence(state).get("rejected_events") or {}
        if rejected.get("has_ever_rejected") is True:
            return False, ("the target is refusing rows — it is failing, not "
                           "merely slow, and batching will not fix that")

        if not self._policy(state):
            return False, ("no batch or flush policy is recorded on the "
                           "target, so there is nothing to tune")

        return True, "sustained backpressure with a healthy target"

    def _policy(self, state):
        props = ((self._evidence(state).get("target_config") or {})
                 .get("properties") or {})
        lowered = {str(k).lower(): v for k, v in props.items()}
        return {k: lowered[k] for k in BATCH_KEYS if lowered.get(k)}

    def plan(self, state):
        source, target = self._source(state), self._target(state)
        current = self._policy(state)
        component = target.get("component")

        # Read the current policy and halve it. Never guess a value - the
        # proposal has to be current -> proposed, or a human cannot judge it.
        proposals = []
        for key, value in current.items():
            proposals.append(f"{key}: {value}  ->  {_halve_policy(value)}")

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=f"halve the flush interval on {component.rsplit('.', 1)[-1]}"
                    if component else "adjust the target's batch policy",
            effect="Smaller, more frequent writes. The target commits sooner, "
                   "so the queue behind it drains. Throughput per batch drops.",
            statements=[f"-- in the Striim UI, on {component}:"] + proposals,
            counts={"source_events_per_second": source.get("event_rate"),
                    "target_events_per_second": target.get("event_rate")},
            warnings=["Smaller batches cost more round trips. If the target "
                      "is slow because it is undersized, this moves the "
                      "problem rather than fixing it."],
        )


def _quote_qualified(table):
    """public.target -> "public"."target" — safe to paste, dialect-correct."""
    parts = [p.strip('"') for p in str(table).strip().split(".") if p.strip()]
    if len(parts) == 1:
        parts = ["public"] + parts
    return ".".join(f'"{p}"' for p in parts)


def _halve_policy(value):
    """eventcount:100000,interval:30s -> eventcount:50000,interval:15s"""
    parts = []
    for chunk in str(value).split(","):
        key, _, raw = chunk.partition(":")
        digits = "".join(c for c in raw if c.isdigit())
        if not digits:
            parts.append(chunk)
            continue
        suffix = raw[len(digits):]
        parts.append(f"{key}:{max(1, int(digits) // 2)}{suffix}")
    return ",".join(parts)


# ============================================= 3. IGNORABLE EXCEPTION

class ProposeIgnorableException(Skill):
    """An app that dies on the same recoverable exception is a support ticket.

    NEVER tier 1. Ignoring an exception means those rows are skipped, and
    whether losing them is acceptable is a business judgement, not an
    operational one. The agent names the class, counts the rows, states the
    proportion, and says plainly what would be lost.
    """

    name = "propose_ignorable_exception"
    zone = "pipeline"
    reversible = True
    max_tier = 2            # never 1, whatever the tier logic decides
    description = ("Propose adding a recurring, isolated exception to the "
                   "application's ignorable list, so it stops terminating on "
                   "it. Use only when the SAME failure has recurred and the "
                   "affected rows are a small fraction of throughput. This "
                   "skips those rows - a human must approve it.")

    def applies(self, state):
        occurrences = state.get("fingerprint_seen", 0)
        if occurrences < 2:
            return False, (f"this fault has been seen {occurrences} time(s) — "
                           f"an exception is only worth ignoring once it has "
                           f"proved recurrent")

        rejected = self._evidence(state).get("rejected_events") or {}
        if rejected.get("exception_store_exists") is not True:
            return False, ("there is no exception store, so the affected rows "
                           "were never captured and cannot be counted")
        if rejected.get("has_ever_rejected") is not True:
            return False, ("nothing has been rejected into the exception "
                           "store, so no rows have actually failed")

        ratio = self._ratio(state)
        if ratio is None:
            return False, ("the proportion of affected rows cannot be "
                           "measured, and an unmeasured proportion is not a "
                           "basis for skipping data")
        if ratio > config.IGNORABLE_MAX_RATIO:
            return False, (f"the failing rows are {ratio:.0%} of throughput — "
                           f"that is systemic, not an isolated bad row, and "
                           f"ignoring it would discard real data")

        return True, f"recurrent and isolated ({ratio:.1%} of rows)"

    def _ratio(self, state):
        rejected = self._evidence(state).get("rejected_events") or {}
        source = self._source(state)
        failing = rejected.get("write_rate")
        total = source.get("event_rate")
        if not total or failing is None:
            return None
        try:
            return min(1.0, float(failing) / float(total))
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    def plan(self, state):
        app = state["app"]
        rejected = self._evidence(state).get("rejected_events") or {}
        errors = (self._evidence(state).get("errors") or {}).get("errors") or []
        ratio = self._ratio(state)
        exception_class = _exception_class(errors) or "the recurring exception"

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=f"stop {app.rsplit('.', 1)[-1]} terminating on "
                    f"{exception_class}",
            effect=f"The pipeline keeps running when {exception_class} "
                   f"occurs, instead of stopping. THE AFFECTED ROWS ARE "
                   f"SKIPPED — they go to the exception store and are not "
                   f"delivered to the target.",
            statements=[
                f"ALTER APPLICATION {app} "
                f"SET EXCEPTIONHANDLER = '{exception_class}:IGNORE';",
                f"-- inspect what would be skipped first:",
                f"SELECT * FROM {rejected.get('store_name', app + '_ExceptionStore')};",
            ],
            counts={"occurrences": state.get("fingerprint_seen", 0),
                    "rejected_rows_per_second": rejected.get("write_rate"),
                    "proportion_of_throughput": (round(ratio, 4)
                                                 if ratio is not None else None)},
            warnings=[
                "This changes what the pipeline does with data, not just how "
                "it runs. Rows matching this exception will be silently "
                "skipped from now on.",
                "Read the exception store before approving. Whether losing "
                "these rows is acceptable is a business decision.",
            ],
        )


def _exception_class(errors):
    """Pull a Java exception class out of the error text, if there is one."""
    for text in errors:
        for token in str(text).replace("(", " ").replace(":", " ").split():
            if "Exception" in token and "." in token:
                return token.strip(".,;")
    return None


# ================================================== 4. MISSING OBJECT

class RecreateMissingTargetObject(Skill):
    """The missing column that stops every write. Now repaired, not proposed.

    DDL used to be propose-only here on the grounds that it cannot carry a
    WHERE clause and so cannot be bounded. That reasoning holds for
    DESTRUCTIVE DDL and it is why _ADDITIVE_DDL exists - but adding a
    column the source already has is bounded by construction: it affects no
    rows, it is reversible with one statement, and leaving it undone means
    every single write continues to fail.
    """

    name = "recreate_missing_target_object"
    zone = "target"
    reversible = True          # ADD COLUMN undoes with DROP COLUMN
    max_tier = 1
    description = ("Add a column or table the target is missing but the "
                   "pipeline needs. Fires on a measured schema difference "
                   "between source and target. Adds only - it will never "
                   "drop or retype anything, and anything destructive is "
                   "written out for a person instead.")

    MARKERS = ("does not exist", "not found", "unknown column",
               "invalid identifier", "no such table", "missing column",
               "undefined column", "invalid object name")

    def applies(self, state):
        """A measured schema difference IS a missing object.

        This used to require error text as its fallback, which on a cluster
        that publishes none for any application (FINDINGS section 12) meant
        the skill could never fire here at all - and it declined with "no
        error text and no schema difference" in the same message whose
        verdict quoted the schema difference. The diff is now the whole
        precondition; error text is a bonus, never a requirement.
        """
        diff = self._evidence(state).get("schema_diff") or {}

        missing = diff.get("missing_in_target") or []
        if missing:
            return True, (f"the target is missing "
                          f"{', '.join(missing)}, measured against the source")

        table = self._evidence(state).get("target_table") or {}
        if table.get("exists") is False:
            return True, f"{table.get('table')} does not exist on the target"

        if diff.get("checked") is False:
            return False, ("the schemas were never compared, so no missing "
                           "object has been identified — that is a gap in "
                           "the evidence, not a clean result")
        if diff.get("matches") is True:
            return False, ("the target's columns match the source, so "
                           "nothing is missing")

        errors = (self._evidence(state).get("errors") or {}).get("errors") or []
        text = " ".join(str(e) for e in errors).lower()
        if text and any(marker in text for marker in self.MARKERS):
            return True, "the target reports a missing object"

        return False, ("no schema difference was found and no error names a "
                       "missing object")

    def plan(self, state):
        """A finished statement, not a description of one.

        "Add the missing column" is not a repair proposal, it is a to-do
        item. The agent has read the error, read the source schema and read
        the target's columns - so it can write the exact statement, say
        which database to run it on, and say how to undo it. What it cannot
        do is execute it.
        """
        evidence = self._evidence(state)
        errors = (evidence.get("errors") or {}).get("errors") or []
        table, column = self._missing(state)
        column_type = self._type_of(state, column) or "text"

        qualified = _quote_qualified(table) if table else '"public"."<table>"'
        source_table = (evidence.get("schema_diff") or {}).get("source_table")

        if column:
            statement = (f'ALTER TABLE {qualified} ADD COLUMN "{column}" '
                         f'{column_type};')
            reversal = f'ALTER TABLE {qualified} DROP COLUMN "{column}";'
            derived = (f"the measured schema difference: the source has "
                       f"{column} as {column_type} and the target has no "
                       f"such column.")
        else:
            # A whole missing table is now reproducible too - its column
            # list comes from db_describe_table on the source, the same
            # source of truth that gets a single column's type right.
            built, provenance = (ddl.create_table_from_source(
                source_table, table) if source_table and table
                else (None, "no source table is known"))
            statement = (built + ";") if built else f"CREATE TABLE {qualified} ( ... );"
            reversal = f"DROP TABLE {qualified};" if built else None
            derived = provenance

        classified = ddl.classify(statement)
        complete = "..." not in statement
        reachable = db.target().configured

        if not complete:
            # Checked before classification: a skeleton with a placeholder
            # column list is a known state with a useful explanation, and
            # "not a DDL form the agent recognises" would be true but
            # useless.
            reason, executable = (
                "the whole table is missing and its column list could not be "
                "read from the source — finish this by hand"), False
        elif classified["kind"] == ddl.REFUSED:
            reason, executable = classified["refusals"][0], False
        elif not reachable:
            reason, executable = (
                "the target database is not reachable from here, so the "
                "statement is written out instead"), False
        elif config.DRY_RUN:
            reason, executable = (
                "dry run — set STRIIM_LIVE=1 to apply this"), False
        else:
            reason, executable = (
                f"adding a column the source already has: it affects no "
                f"existing rows and undoes with one statement"), True

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=bool(reversal),
            summary=(f"add the missing {column} column to "
                     f"{table}" if column else f"create {table} on the target"),
            effect=f"The target gains {column or 'the missing object'}, which "
                   f"the pipeline is already sending. No existing row is "
                   f"read, changed or deleted.",
            statements=[statement] +
                       [f"-- from: {str(e)[:160]}" for e in errors[:1]],
            command=statement,
            run_on=self._database_name(state),
            derived_from=derived,
            reversal=reversal,
            counts={},
            executable=executable,
            reason=reason,
            warnings=([] if executable else
                      ["Check the type against the source before running it."]),
        )

    def execute(self, state, plan, services):
        """Run it through the DDL layer, which owns the guarantees.

        Everything about capture, provenance, refusal and verification
        lives in one module rather than in each caller - so a destructive
        statement reaching here through some future path gets the same
        backup and the same restore statement as one arriving through this
        one.
        """
        statement = (plan.command or "").strip()

        prepared = ddl.plan(
            statement,
            derived_from=plan.derived_from,
            evidence=self._evidence(state),
            incident=state.get("fingerprint"))

        if not prepared["runnable"]:
            raise target_repair.RepairRefused(prepared["reason"])

        ran = ddl.execute(prepared, services=services,
                          thread=state.get("slack_thread_ts"))

        table, column = self._missing(state)
        did = [f"added {column} ({self._type_of(state, column) or 'text'})"
               if column else f"recreated {table} from the source schema"]

        outcome = {"ran": statement, "reversal": plan.reversal,
                   "rows_affected": 0, "did": did,
                   "kind": ran["kind"], "backup_table": ran.get("backup_table"),
                   "restore": ran.get("restore"),
                   "verified": ran.get("verified"),
                   "note": (ran.get("verified") or {}).get("detail")
                           or "the change was applied"}

        # Restoring the column is only half the repair. Every row that
        # existed before it was dropped now holds NULL - those values went
        # with the column - and change capture delivers only new changes,
        # so nothing will ever fill them in. The schema would be right and
        # the data quietly wrong, with the pipeline reporting healthy.
        outcome["backfill"] = self._backfill(state, table, column, services)
        if outcome["backfill"] and outcome["backfill"].get("rows_filled"):
            outcome["did"].append(
                f"backfilled {outcome['backfill']['rows_filled']:,} rows "
                f"from the source")
        return outcome

    def _backfill(self, state, table, column, services):
        """Fill in whatever the structural repair left empty.

        Which scope applies follows from what was repaired: a restored
        COLUMN leaves rows that exist with one value missing; a recreated
        TABLE leaves no rows at all. The second is the worse of the two -
        change capture carries changes forward only, so a table recreated
        today holds nothing that happened before today.
        """
        source_table = (self._evidence(state).get("schema_diff") or {}).get(
            "source_table")
        if not source_table:
            return {"runnable": False,
                    "reason": "the source table is not known, so there is "
                              "nothing to copy values from",
                    "rows_filled": 0}

        found = backfill.plan(source_table, table, column,
                              incident=state.get("fingerprint"))
        thread = state.get("slack_thread_ts")

        if not found["runnable"]:
            # Not a failure of the repair - the column is back either way.
            # But a column full of NULLs is a fact somebody needs told.
            if services:
                services.log(f"    {backfill.describe(found)}")
            return {**found, "rows_filled": 0}

        if services:
            services.log(f"    {backfill.describe(found)}")

        try:
            result = backfill.run(found)
        except Exception as error:
            if services and thread:
                services.notifier.reply(
                    thread, f"⚠ the column was added but the backfill failed: "
                            f"{error}. Every pre-existing row still has a "
                            f"null {column}.", kind="failure")
            return {**found, "rows_filled": 0,
                    "error": f"{type(error).__name__}: {error}"}

        if services:
            services.log(f"    {result['note']}")
        return result

    def _missing(self, state):
        """The table and column the writer cannot find.

        The measured diff first, because it is a fact. The error text is
        the fallback, because a regex over a vendor's prose is a guess.
        """
        evidence = self._evidence(state)

        diff = evidence.get("schema_diff") or {}
        if diff.get("missing_in_target"):
            return diff["target_table"], diff["missing_in_target"][0]

        errors = " ".join(str(e) for e in
                          ((evidence.get("errors") or {}).get("errors") or []))
        column = None
        match = re.search(r'column ["\']?(\w+)["\']?', errors, re.I)
        if match:
            column = match.group(1)

        table = ((evidence.get("target_table") or {}).get("table")
                 or (evidence.get("target_config") or {}).get("expects"))
        return table, column

    def _type_of(self, state, column):
        """The source's type for that column, if we were able to read it."""
        diff = self._evidence(state).get("schema_diff") or {}
        from_diff = (diff.get("source_columns") or {}).get(column)
        if from_diff:
            return from_diff

        described = self._evidence(state).get("source_table") or {}
        for entry in described.get("columns") or []:
            if entry.get("column_name") == column:
                return entry.get("data_type")
        return None

    def _database_name(self, state):
        props = ((self._evidence(state).get("target_config") or {})
                 .get("properties") or {})
        url = str({str(k).lower(): v for k, v in props.items()}
                  .get("connectionurl") or "")
        name = url.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
        return f"{name} (the target)" if name else "the target database"


# ======================================================== 5. DEDUPLICATE

class WidenTargetColumn(Skill):
    """A target column too small for what the source sends.

    Runs without asking, and that is the right call: widening loses
    nothing, the pipeline is already broken, and there is nobody awake to
    confirm it at three in the morning. The narrow column is rejecting
    every row that does not fit, and each minute spent waiting for a human
    is a minute of data going into the exception store.
    """

    name = "widen_target_column"
    zone = "target"
    reversible = False         # narrowing back could truncate
    max_tier = 1
    description = ("Widen a target column that is too small for the values "
                   "the source sends - varchar(3) to text, int to bigint. "
                   "Fires on a measured type difference, never on a length "
                   "read out of an error message. Nothing is lost, so it "
                   "runs on its own.")

    def applies(self, state):
        diff = self._evidence(state).get("schema_diff") or {}

        if diff.get("checked") is False:
            return False, ("the schemas were never compared, so no type "
                           "difference has been measured — that is a gap in "
                           "the evidence, not a clean result")

        widenable = [d for d in diff.get("differing_types") or []
                     if d.get("widening") is True]
        if not widenable:
            narrower = [d for d in diff.get("differing_types") or []
                        if d.get("widening") is False]
            if narrower:
                return False, (f"{narrower[0]['column']} is wider on the "
                               f"target than the source, so it accepts "
                               f"everything sent — that is not what is "
                               f"rejecting writes")
            return False, "no column type differs between source and target"

        self._chosen = widenable[0]
        return True, (f"{widenable[0]['column']} is "
                      f"{widenable[0]['target_type']} on the target and "
                      f"{widenable[0]['source_type']} on the source")

    def _difference(self, state):
        diff = self._evidence(state).get("schema_diff") or {}
        for entry in diff.get("differing_types") or []:
            if entry.get("widening") is True:
                return entry
        return None

    def plan(self, state):
        diff = self._evidence(state).get("schema_diff") or {}
        change = self._difference(state)
        table = diff.get("target_table") or "<target table>"

        if not change:
            return RepairPlan(
                skill=self.name, zone=self.zone, reversible=False,
                summary="widen the target column", effect="",
                executable=False, reason="no widening was measured")

        statement = (f'ALTER TABLE {_quote_qualified(table)} ALTER COLUMN '
                     f'"{change["column"]}" TYPE {change["source_type"]};')

        reachable = db.target().configured
        if config.DRY_RUN:
            reason, executable = "dry run — set STRIIM_LIVE=1 to apply", False
        elif not reachable:
            reason, executable = ("the target database is not reachable from "
                                  "here"), False
        else:
            reason, executable = (
                f"widening {change['target_type']} to "
                f"{change['source_type']} loses nothing, and every row too "
                f"long for the current column is being refused"), True

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=False,
            summary=f"widen {change['column']} to {change['source_type']} "
                    f"on {table}",
            effect=f"{change['column']} accepts the values the source is "
                   f"already sending. No existing value is read, changed or "
                   f"truncated.",
            statements=[statement],
            command=statement,
            run_on=_database_name(self._evidence(state)),
            derived_from=(f"the measured schema difference: {change['column']} "
                          f"is {change['source_type']} on the source and "
                          f"{change['target_type']} on the target, both read "
                          f"from information_schema"),
            reversal=(f'ALTER TABLE {_quote_qualified(table)} ALTER COLUMN '
                      f'"{change["column"]}" TYPE {change["target_type"]};   '
                      f'-- WOULD TRUNCATE anything longer'),
            executable=executable,
            reason=reason,
        )

    def execute(self, state, plan, services):
        ran = _run_ddl(self, state, plan, services)
        change = self._difference(state) or {}
        return {"ran": plan.command, "rows_affected": 0,
                "did": [f"widened {change.get('column')} to "
                        f"{change.get('source_type')}"],
                "kind": ran["kind"], "verified": ran.get("verified"),
                "note": (ran.get("verified") or {}).get("detail")
                        or "the column was widened"}


class RelaxBlockingColumn(Skill):
    """A target column that rejects every write the source can produce.

    NOT NULL, no default, and no counterpart on the source - so every row
    the pipeline sends is refused. Dropping the constraint is preferred
    over dropping the column: the column may be somebody else's, and
    relaxing it loses nothing that is already there.

    This one ASKS, because a constraint is a decision somebody made.
    """

    name = "relax_blocking_column"
    zone = "target"
    reversible = True
    max_tier = 1
    description = ("Drop NOT NULL from a target column that the source "
                   "cannot supply, so writes stop being rejected. Preferred "
                   "over dropping the column, which would lose whatever is "
                   "already in it. Needs a human to approve it in the "
                   "thread.")

    def applies(self, state):
        diff = self._evidence(state).get("schema_diff") or {}
        if diff.get("checked") is False:
            return False, ("the schemas were never compared, so no blocking "
                           "column has been identified")

        blocking = diff.get("blocking_in_target") or []
        if not blocking:
            return False, ("no target column is NOT NULL without a default "
                           "and without a source counterpart")
        return True, (f"{blocking[0]['column']} is "
                      f"{blocking[0]['why']}")

    def plan(self, state):
        diff = self._evidence(state).get("schema_diff") or {}
        blocking = (diff.get("blocking_in_target") or [{}])[0]
        table = diff.get("target_table") or "<target table>"
        column = blocking.get("column")

        qualified = _quote_qualified(table)
        statement = (f'ALTER TABLE {qualified} ALTER COLUMN "{column}" '
                     f'DROP NOT NULL;')

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=f"drop NOT NULL from {column} on {table}",
            effect=f"{column} starts accepting rows without a value. Every "
                   f"row already in the table keeps the value it has - "
                   f"nothing is read, changed or removed.",
            statements=[statement],
            command=statement,
            run_on=_database_name(self._evidence(state)),
            derived_from=(f"the measured schema difference: {column} is "
                          f"{blocking.get('type')} NOT NULL on the target "
                          f"with no default, and the source sends no such "
                          f"column"),
            reversal=(f'ALTER TABLE {qualified} ALTER COLUMN "{column}" '
                      f'SET NOT NULL;   -- fails if any row is null by then'),
            executable=not config.DRY_RUN and db.target().configured,
            reason=(f"dropping the constraint is preferred over dropping "
                    f"{column} — the column may be somebody else's, and "
                    f"relaxing it loses nothing"),
            warnings=["Dropping the column instead would lose whatever is "
                      "already in it. This does not."],
        )

    def execute(self, state, plan, services):
        ran = _run_ddl(self, state, plan, services)
        diff = self._evidence(state).get("schema_diff") or {}
        column = (diff.get("blocking_in_target") or [{}])[0].get("column")
        return {"ran": plan.command, "rows_affected": 0,
                "did": [f"dropped NOT NULL from {column}"],
                "kind": ran["kind"], "verified": ran.get("verified"),
                "note": (ran.get("verified") or {}).get("detail")
                        or "the constraint was relaxed"}


def _database_name(evidence):
    props = ((evidence.get("target_config") or {}).get("properties") or {})
    url = str({str(k).lower(): v for k, v in props.items()}
              .get("connectionurl") or "")
    name = url.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
    return f"{name} (the target)" if name else "the target database"


def _run_ddl(skill, state, plan, services):
    """Every skill's DDL goes through here, so the gate is in one place.

    Additive changes run. Destructive ones stop and ask in the thread -
    see nodes.request_approval, which raises rather than returning, so a
    caller cannot accidentally carry on as though the change had happened.
    """
    from . import nodes

    prepared = ddl.plan(
        (plan.command or "").strip(),
        derived_from=plan.derived_from,
        evidence=skill._evidence(state),
        incident=state.get("fingerprint"))

    if not prepared["runnable"]:
        raise target_repair.RepairRefused(prepared["reason"])

    if prepared["kind"] == ddl.DESTRUCTIVE:
        nodes.request_approval(state, services, prepared, skill.name)

    return ddl.execute(prepared, services=services,
                       thread=state.get("slack_thread_ts"))


class DedupeTargetRows(Skill):
    """The skill that closes the loop on this project's central finding.

    We showed that a re-read silently duplicates every row and no alert
    fires. This is the other half: finding them and offering to remove them.
    """

    name = "dedupe_target_rows"
    zone = "target"
    reversible = False
    max_tier = 1            # only under the row threshold; see plan()
    description = ("Remove rows a re-read duplicated into the target. Use "
                   "only when a re-read is confirmed AND the target has a key "
                   "to match duplicates on. Impossible on append-only targets "
                   "- files, object storage, lakes in append mode - because "
                   "there is nothing to match on.")

    def applies(self, state):
        target = self._target(state)
        behaviour = (target.get("profile") or {}).get("behaviour")

        if behaviour == "append_only_writer":
            # This sentence is itself a finding worth showing.
            return False, ("this target appends and has no keys, so "
                           "duplicates cannot be identified, let alone "
                           "removed. Nothing can deduplicate a file after "
                           "the fact — the damage from a re-read here is "
                           "permanent, which is exactly why the restart is "
                           "refused rather than repaired")

        if behaviour != "transactional_writer":
            described = behaviour or "an unrecognised connector"
            return False, (f"this target is {described}, and deduplication "
                           f"needs a transactional target with a key")

        key = config.TARGET_PRIMARY_KEY
        if not key:
            return False, ("no primary key is configured for the target "
                           "(TARGET_PRIMARY_KEY), and without one there is no "
                           "safe way to tell a duplicate from a real row")

        if not state.get("reread_confirmed"):
            return False, ("no re-read has been confirmed — deduplicating "
                           "without one would delete rows that belong there")

        return True, "a re-read is confirmed and the target has a key"

    def plan(self, state):
        table = config.TARGET_TABLE or "<target table>"
        key = config.TARGET_PRIMARY_KEY or "<key column>"

        # Bounded by construction: keeps the first of each key, removes the
        # rest. The count comes from the same predicate as the delete.
        statement = (f"DELETE FROM {table} WHERE ctid NOT IN "
                     f"(SELECT MIN(ctid) FROM {table} GROUP BY {key})")

        plan = RepairPlan(
            skill=self.name, zone=self.zone, reversible=False,
            summary=f"remove duplicate rows from {table}",
            effect=f"One row survives per {key}. Every later copy is deleted.",
            statements=[statement],
            warnings=["This deletes data. Take a backup of the affected rows "
                      "first if you can."],
        )

        # Count before you change. An uncounted write is not an informed one.
        inspected = target_repair.inspect(statement)
        if inspected["count_query"]:
            try:
                plan.counts["rows_to_delete"] = target_repair.count_rows(
                    inspected["count_query"])
            except Exception as error:
                plan.reason = (f"the affected rows could not be counted "
                               f"({error}), so this is proposed, not run")
                return plan

        rows = plan.counts.get("rows_to_delete")
        if rows is None:
            plan.reason = "the affected rows were not counted, so this is "\
                          "proposed rather than run"
        elif rows > config.TARGET_MAX_ROWS:
            plan.reason = (f"{rows:,} rows is over the "
                           f"{config.TARGET_MAX_ROWS:,} limit for an "
                           f"unattended write")
        else:
            plan.executable = True
            plan.reason = f"bounded delete of {rows:,} duplicate rows"
        return plan

    def execute(self, state, plan, services):
        return target_repair.execute({
            "will_execute": True, "sql": plan.statements[0],
            "rows_affected": plan.counts.get("rows_to_delete"),
            "incident": state.get("fingerprint"), "cause": plan.summary,
        })


# ===================================================== 6. PARTIAL BATCH

class ClearPartialBatch(Skill):
    """A resume that would collide with a half-written batch."""

    name = "clear_partial_batch"
    zone = "target"
    reversible = False
    max_tier = 2
    description = ("Remove the rows of a batch that was half applied when "
                   "the pipeline died, so a resume does not collide with "
                   "them. Needs the batch to be identifiable by a bounded "
                   "predicate.")

    def applies(self, state):
        target = self._target(state)
        behaviour = (target.get("profile") or {}).get("behaviour")
        if behaviour != "transactional_writer":
            return False, ("a partial batch can only be cleared from a "
                           "transactional target")
        if not config.TARGET_TABLE or not config.TARGET_BATCH_COLUMN:
            return False, ("the target table and batch column are not "
                           "configured, so the batch cannot be bounded")
        if not state.get("partial_batch_id"):
            return False, ("no partial batch has been identified — clearing "
                           "an unidentified one would delete good rows")
        return True, "a half-written batch is identified and bounded"

    def plan(self, state):
        table = config.TARGET_TABLE
        column = config.TARGET_BATCH_COLUMN
        batch = state.get("partial_batch_id")
        statement = f"DELETE FROM {table} WHERE {column} = '{batch}'"

        plan = RepairPlan(
            skill=self.name, zone=self.zone, reversible=False,
            summary=f"clear the half-written batch {batch}",
            effect=f"Rows from batch {batch} are removed so the resume can "
                   f"rewrite them cleanly.",
            statements=[statement],
            warnings=["Only run this if the resume will replay that batch. "
                      "Otherwise the rows are simply lost."],
        )

        inspected = target_repair.inspect(statement)
        try:
            plan.counts["rows_to_delete"] = target_repair.count_rows(
                inspected["count_query"])
            plan.reason = f"bounded delete of batch {batch}"
        except Exception as error:
            plan.reason = f"the rows could not be counted ({error})"
        return plan


# =========================================================== 7. ESCALATE

class EscalateSourceProblem(Skill):
    """Never acts. Its whole value is the specificity of what it writes.

    "Check the source system" is useless at 3am. "AI.csv is missing from
    UploadedFiles/sarah_moolai/ and the reader had got to byte 171,000 of
    it" is something a person can act on without logging in.
    """

    name = "escalate_source_problem"
    zone = "source"
    reversible = True       # nothing is done, so nothing needs undoing
    max_tier = 3
    description = ("Write a precise escalation for a fault in the source - a "
                   "missing file, an unreachable host, a rejected "
                   "credential, a purged replication slot. The agent NEVER "
                   "writes to a source. Use this whenever the fix is in "
                   "somebody else's system.")

    def applies(self, state):
        if not self._evidence(state).get("source_config"):
            return False, ("the source configuration could not be read, so "
                           "there is nothing specific to escalate")
        return True, "the fault is in the source"

    def plan(self, state):
        evidence = self._evidence(state)

        # A replication slot that is GONE is categorically different from
        # one that is merely inactive: the stored position went with it,
        # and no restart can recover it. Restarting reads from wherever
        # Striim decides to begin - most likely the beginning - and
        # duplicates everything already delivered.
        slot = evidence.get("replication_slot") or {}
        if slot.get("configured") and slot.get("exists") is False:
            return self._slot_gone(state, slot)

        source_config = evidence.get("source_config") or {}
        source = self._source(state)
        probe = evidence.get("dependency")
        position = source_config.get("read_position") or {}
        checkpoints = evidence.get("checkpoints") or {}

        expects = source_config.get("expects")
        adapter = source_config.get("adapter") or (
            source.get("profile") or {}).get("adapter_guess")

        detail = []
        if expects:
            detail.append(f"Expects: {expects}")
        if adapter:
            detail.append(f"Adapter: {adapter}")
        if position.get("offset_end_bytes"):
            detail.append(f"Read position: byte "
                          f"{position['offset_end_bytes']:,} of "
                          f"{position.get('source_name')}")
        if checkpoints.get("last_checkpoint_time"):
            detail.append(f"Last checkpoint: "
                          f"{checkpoints['last_checkpoint_time']}")
        if probe:
            detail.append(probes.describe_reachability(probe))

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=_source_action(expects, probe, evidence),
            effect="Nothing. The agent does not write to a source, ever — "
                   "it is somebody else's system of record and a wrong write "
                   "there is unrecoverable and invisible to us.",
            statements=[f"-- for a person, not for the agent:"] + detail,
            counts={},
            executable=False,
            reason="source faults are always escalated, never repaired",
        )


    def _slot_gone(self, state, slot):
        """The zone-3 case, written so nobody has to go and look anything up.

        Every noun specific: which slot, which host, how many rows are
        already in the target and would be delivered a second time, and
        the exact statement that recreates it - with the right plugin,
        because a slot recreated with pgoutput fails only at runtime and
        looks fine until then.
        """
        evidence = self._evidence(state)
        name = (_configured_slot_name(evidence) or "the replication slot")
        endpoint = (evidence.get("source_endpoint") or {})
        where = (f"{endpoint.get('host')}:{endpoint.get('port')}/"
                 f"{endpoint.get('database')}"
                 if endpoint.get("host") else "the source database")

        rows = (evidence.get("target_rows") or {}).get("rows")
        delivered = (f" and duplicate the {rows:,} rows already in the target"
                     if rows else " and duplicate everything already delivered")

        plugin = _configured_plugin(evidence) or "wal2json"
        recreate = (f"SELECT pg_create_logical_replication_slot"
                    f"('{name}', '{plugin}');")

        return RepairPlan(
            skill=self.name, zone=self.zone, reversible=True,
            summary=f"recreate the {name} slot on the source, then run a "
                    f"fresh initial load",
            effect="Nothing. The slot is on the SOURCE, which this agent "
                   "never writes to — it is somebody else's system of "
                   "record and a wrong write there is unrecoverable.",
            statements=[
                f"-- run this on the source ({where}), not on the target:",
                recreate,
                f"-- then start a fresh initial load; the position the "
                f"pipeline had is gone and cannot be recovered.",
            ],
            counts={"rows_already_in_target": rows},
            executable=False,
            reason=f"the replication slot {name} no longer exists on {where}, "
                   f"so there is no position to resume from. Restarting "
                   f"would re-read from the beginning{delivered}.",
            warnings=[
                f"The plugin must be {plugin}. A slot recreated with the "
                f"wrong one is accepted here and fails only at runtime.",
            ],
        )


def _configured_slot_name(evidence):
    props = ((evidence.get("source_config") or {}).get("properties") or {})
    lowered = {str(k).lower(): v for k, v in props.items()}
    return lowered.get("replicationslotname") or lowered.get("replicationslot")


def _configured_plugin(evidence):
    """The plugin the READER is configured for, not a guess.

    A slot recreated with the wrong plugin is accepted by Postgres and
    fails only when Striim tries to read it, which is the worst time to
    find out.
    """
    props = ((evidence.get("source_config") or {}).get("properties") or {})
    lowered = {str(k).lower(): str(v) for k, v in props.items()}
    declared = (lowered.get("connectorclass") or lowered.get("plugin")
                or lowered.get("decoderplugin") or "")
    return declared if declared in ("wal2json", "pgoutput") else None


def _source_action(expects, probe, evidence=None):
    """The single next action, and only where the evidence supports one.

    "restore public.source" was being produced for a stalled CDC pipeline
    whose source table was present and perfectly healthy - because
    `expects` is set for every reader, and the old code read "I know what
    it reads" as "that thing is missing". Telling somebody to restore a
    table that is already there is worse than saying nothing: they go and
    look, find it, and stop trusting the next incident.
    """
    evidence = evidence or {}

    slot = evidence.get("replication_slot") or {}
    if slot.get("exists") is False:
        return "recreate the replication slot, then run a fresh initial load"
    if slot.get("inactive"):
        return (f"restart the pipeline so it reattaches to "
                f"{', '.join(slot['inactive'])}")

    if probe and probe.get("reachable") is False:
        if probe.get("kind") == "tcp":
            return f"bring {probe['target']} back up, then restart the pipeline"
        return f"restore {probe.get('target') or expects}, then restart"

    # Only claim something is missing where something is KNOWN to be
    # missing - the error text named it, or a probe looked and did not
    # find it. Otherwise say what was checked and hand it over honestly.
    errors = " ".join(str(e) for e in
                      ((evidence.get("errors") or {}).get("errors") or []))
    if expects and errors and str(expects).rsplit(".", 1)[-1] in errors:
        return f"restore {expects}, then restart the pipeline"

    # NO APOLOGY. "the agent could not determine what is wrong with it from
    # here" told a reader nothing they could use, and it appeared even on
    # incidents whose cause was perfectly well established.
    #
    # If something genuinely is unknown, name WHAT is unknown and the one
    # command that would settle it. That is a handover; the other was a
    # shrug with a full stop after it.
    if expects:
        return (f"run `SELECT count(*) FROM {expects}` on the source — if it "
                f"answers, the table is readable and the fault is between "
                f"there and Striim")
    return ("name the source object in the pipeline's configuration, then "
            "check it is readable with the credentials Striim uses")


# ============================================================== REGISTRY

SKILLS = {skill.name: skill for skill in (
    WaitAndResume(),
    TuneBatchPolicy(),
    ProposeIgnorableException(),
    RecreateMissingTargetObject(),
    WidenTargetColumn(),
    RelaxBlockingColumn(),
    DedupeTargetRows(),
    ClearPartialBatch(),
    EscalateSourceProblem(),
)}


def catalogue():
    """The skill list as the model sees it, for the system prompt."""
    lines = []
    for skill in SKILLS.values():
        lines.append(f'  "{skill.name}" ({skill.zone} zone) — '
                     f"{skill.description}")
    return "\n".join(lines)


def choose(state, suggested):
    """Take the model's suggestion and hold it to the evidence.

    Returns (skill, plan, dropped_reason). Exactly one of skill/dropped
    is meaningful:

      skill + plan     preconditions passed; the plan goes in the incident
      dropped_reason   why it does not apply - which is ALSO worth saying,
                       because "considered dedupe, but the target has no
                       primary key" tells a human something about their
                       pipeline they probably did not know
    """
    if not suggested:
        return None, None, None

    skill = SKILLS.get(suggested)
    if skill is None:
        return None, None, (f"the diagnosis suggested a repair called "
                            f"'{suggested}', which does not exist")

    # Zone check first, and it is absolute. A target repair proposed for a
    # source fault is refused before its preconditions are even read.
    zone = (state.get("verdict") or {}).get("fault_zone", "unknown")
    if skill.zone != zone:
        return None, None, (f"considered {skill.name}, but it repairs the "
                            f"{skill.zone} and this fault is in the {zone}")

    ok, why = skill.applies(state)
    if not ok:
        return None, None, f"considered {skill.name}, but {why}"

    try:
        plan = skill.plan(state)
    except Exception as error:
        return None, None, (f"considered {skill.name}, but planning it failed "
                            f"({type(error).__name__}: {error})")

    return skill, plan, None


def describe(plan):
    """A repair plan, in the words that go into the Slack thread."""
    if plan is None:
        return None

    lines = [f"*{plan.summary}*", f"_{plan.effect}_"]

    if plan.statements:
        body = "\n".join(str(s) for s in plan.statements)
        lines.append(f"```{body}```")

    measured = {k: v for k, v in plan.counts.items() if v is not None}
    if measured:
        lines.append("   " + ", ".join(f"{k.replace('_', ' ')}: {v}"
                                       for k, v in measured.items()))

    for warning in plan.warnings:
        lines.append(f"   ⚠ {warning}")

    if not plan.executable:
        # Only claim a refusal when there is a reason for one. A plan with
        # neither is a plan whose tier has not been decided yet, and saying
        # "not running it" next to an action that is about to run reads as
        # a contradiction - which it is.
        if plan.reason:
            lines.append(f"   ⛔ not running it: {plan.reason}")
        lines.append("   → Do this: review the above and apply it yourself "
                     "if it is right")
    elif plan.reason:
        lines.append(f"   _{plan.reason}_")

    if not plan.reversible:
        lines.append("   ⚠ This cannot be undone.")

    return "\n".join(lines)
