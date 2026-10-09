"""
The nodes
=========

Eight of them, and only ONE uses the model.

    WATCH     poll the status                          no model
    TRIAGE    is this a fault, and is it new?          no model
    EVIDENCE  wait for the error text if it is coming  no model
    DIAGNOSE  tool-calling loop, the model drives      YES
    DECIDE    apply the guardrails, choose a tier      no model
    ESCALATE  write the incident and post it           formatting only
    ACT       run the remediation command              no model
    VERIFY    did data actually move?                  no model

That split is the whole design:

    The model recommends. The code decides.

The model can be completely wrong and nothing bad happens, because it has
no hands. Every guardrail lives in Python where it cannot be argued with,
and the model's suggested tier can only ever be lowered, never raised.
"""

import json
import time

from . import config
from . import db
from . import ddl
from . import probes
from . import prompts
from . import skills as skill_layer
from . import slack_io
from . import striim_actions as act
from . import striim_tools as st
from . import target_repair
from . import toolbox
from .profiles import profile_for
from .state import fingerprint, short_name

# Sources where a restart can re-read everything it has already read. For
# these, a missing or untrustworthy position is not a detail - it is the
# difference between fixing a pipeline and silently doubling a table.
RE_READ_BEHAVIOURS = {"file_reader", "batch_reader", "spreadsheet_reader"}

# Sources that cannot re-read, only carry on or miss. Restarting one of
# these without a stored position loses whatever happened while it was
# down, and nothing anywhere will say so.
RESUMABLE_BEHAVIOURS = {"cdc_reader", "stream_reader"}


class Services:
    """Everything the nodes need that is not state.

    Passed in rather than imported so the tests can hand over a fake model,
    a fixture-backed Striim and a Notifier that only prints.
    """

    def __init__(self, striim, model=None, notifier=None, ledger=None,
                 evidence_wait=None, evidence_attempts=None,
                 verify_wait=None, verbose=True):
        self.striim = striim
        self.model = model
        self.notifier = notifier or slack_io.Notifier()
        self.ledger = ledger if ledger is not None else act.Ledger()
        self.evidence_wait = (config.EVIDENCE_WAIT_SECONDS
                              if evidence_wait is None else evidence_wait)
        self.evidence_attempts = (config.EVIDENCE_ATTEMPTS
                                  if evidence_attempts is None
                                  else evidence_attempts)
        self.verify_wait = (config.VERIFY_WAIT_SECONDS
                            if verify_wait is None else verify_wait)
        self.verbose = verbose
        # Faults already reported, so the same one is not raised every
        # minute for an hour. Keyed by app.
        self.open_incidents = {}

        # Applications this agent took apart and could not put back. They
        # sit in DEPLOYED or CREATED, which are calm states, so nothing
        # else would ever notice them again.
        self.left_undeployed = {}

        # Where each pipeline's read position was, the last few times we
        # looked. A stalled pipeline reports RUNNING and checkpoints
        # happily at the same position forever, so the only way to see it
        # is to remember where it was last time.
        self.observations = {}

        # Destructive repairs waiting for someone to say yes, keyed by the
        # Slack thread they were asked in. Plain data, so the answer can
        # arrive on a completely different call stack minutes later.
        self.pending = {}

    def pending_for(self, thread):
        found = self.pending.get(thread)
        return found if found and found["state"] == "waiting" else None

    def expire_approvals(self, now=None):
        """Give up on anything nobody answered, and say so on the parent.

        Two things happen here, and both are needed. The parent is rewritten,
        because until it is it still reads "Reply *approve* to run it ... I
        will give up in 15 minutes" - an offer that has already been
        withdrawn, sitting there indefinitely. And a reply goes in the
        thread, because editing a Slack message notifies nobody: the person
        who was being waited on would never learn they had been waited on.
        """
        now = now or time.time()
        minutes = config.APPROVAL_TIMEOUT_SECONDS // 60
        gone = []
        for thread, pending in list(self.pending.items()):
            if pending["state"] != "waiting":
                continue
            waited = now - pending["asked_at"]
            if waited < config.APPROVAL_TIMEOUT_SECONDS:
                continue

            pending["state"] = "expired"
            act.audit({"approval": "expired", "app": pending["app"],
                       "incident": pending["fingerprint"],
                       "statement": pending["summary"],
                       "waited_seconds": round(waited)})

            self.notifier.rewrite(thread, slack_io.compose_incident(
                pending["app"], pending.get("verdict"),
                _expired_record(pending, minutes)))
            self.notifier.set_state(thread, slack_io.NEEDS_HUMAN)
            self.notifier.reply(
                thread,
                f"⌛ {minutes} minutes up — nobody approved it, so I stopped "
                f"waiting. Nothing was changed and no backup was taken. The "
                f"statement is on the message above.",
                kind="failure")
            gone.append(thread)
        return gone

    def log(self, text):
        if self.verbose:
            print(text)


# ================================================================ WATCH

def watch(state, services):
    """Read the current status. One health call, no interpretation."""
    app = state["app"]
    status = st.get_status(services.striim, app)

    services.log(f"[{time.strftime('%H:%M:%S')}] {short_name(app)} "
                 f"{status['status']}")

    return {"status": status["status"],
            "evidence": {**state.get("evidence", {}), "status": status},
            "started_at": state.get("started_at") or time.time()}


# =============================================================== TRIAGE

def triage(state, services):
    """Is this real, and is it new?

    Three ways to end here without saying anything, and all three matter
    more than they look. A channel that fires on healthy pipelines, or on
    pipelines someone stopped on purpose, or every sixty seconds for the
    same fault, is a channel people mute - and a muted channel is worse
    than no channel, because everyone believes it is working.
    """
    app = state["app"]
    status = state["status"]
    open_incident = services.open_incidents.get(app)

    # An application the AGENT left half-way through a property change is
    # an incident, not silence. DEPLOYED and CREATED are both calm states,
    # so without this the agent takes a pipeline apart, fails to put it
    # back, and then never mentions it again - which is the worst failure
    # mode available to it, because it is self-inflicted and quiet.
    if status in ("DEPLOYED", "CREATED") and app in services.left_undeployed:
        services.log(f"    {short_name(app)} is {status} and the agent left "
                     f"it that way")
        return {"silent": False,
                "evidence": {**state.get("evidence", {}),
                             "left_undeployed": services.left_undeployed[app]}}

    # A pipeline can report RUNNING and move nothing. The status word is
    # not the whole answer, so a calm status that LOOKS stalled goes on to
    # EVIDENCE rather than stopping here - where the slot and the position
    # can settle it. Nothing is posted unless DECIDE agrees, so silence
    # when healthy is unaffected; this only costs a look.
    if status not in st.FAULT_STATES and _worth_a_closer_look(state, services):
        services.log(f"    {status}, but nothing is moving — looking closer")
        return {"silent": False, "suspected_stall": True,
                "evidence": {**state.get("evidence", {}),
                             "history": st.get_state_history(services.striim,
                                                             app)}}

    # Recovered - by us, by someone else, or by itself. Close the thread.
    if status not in st.FAULT_STATES:
        if open_incident and status in ("RUNNING", "COMPLETED"):
            services.notifier.set_state(open_incident["thread"],
                                        slack_io.RESOLVED)
            services.notifier.reply(
                open_incident["thread"],
                f"{slack_io.RESOLVED}  *{short_name(app)}* is {status} again.",
                kind="outcome")
            services.open_incidents.pop(app, None)
        return {"silent": True,
                "reason": f"{status} is not a fault"}

    # Somebody asked for this. A clean shutdown passes through STOPPING; if
    # the history says it did, this is not an incident. Note that the test
    # only runs one way - see get_state_history for why applying it to a
    # start is a category error.
    history = st.get_state_history(services.striim, app)
    if history.get("looks_deliberate") is True:
        return {"silent": True,
                "reason": "someone did this deliberately",
                "evidence": {**state.get("evidence", {}), "history": history}}

    # A human put ✅ on this one. They have said they are dealing with it,
    # so stop. Re-raising the same fault under someone who just closed it
    # is how an agent gets muted, and a muted channel is worse than none.
    if open_incident and open_incident.get("closed_by_human"):
        return {"silent": True,
                "reason": "a human marked this resolved — standing down"}

    # Already raised, still broken. Nothing new to say.
    if open_incident and open_incident["status"] == status:
        return {"silent": True,
                "reason": f"already open since "
                          f"{round(time.time() - open_incident['at'])}s ago"}

    services.log(f"    fault: {status} — investigating")
    return {"silent": False,
            "evidence": {**state.get("evidence", {}), "history": history}}


def _worth_a_closer_look(state, services):
    """Cheap, health-record-only test for a pipeline that has gone quiet.

    Deliberately narrow. TRIAGE runs before any database call, so this
    knows nothing about the replication slot - it only asks whether both
    ends have been silent for longer than this kind of connector goes
    silent. A file reader that has finished never trips it; a CDC source
    on a live database does.

    Being selective matters: everything past this point costs a model call.
    """
    if state.get("status") not in ("RUNNING", "DEPLOYED"):
        return False

    flow = st.get_flow_stats(services.striim, state["app"])
    source, target = flow.get("source") or {}, flow.get("target") or {}

    if (source.get("profile") or {}).get("silence_is_normal") is not False:
        return False

    quiet = [n for n in (source.get("seconds_since_last_event"),
                         target.get("seconds_since_last_write"))
             if n is not None]
    return bool(quiet) and min(quiet) > config.STALL_SILENCE_SECONDS


# ============================================================= EVIDENCE

def evidence(state, services):
    """Wait for the error text, then gather the baseline.

    Error text is published SEPARATELY from the status change, and later.
    Diagnosing the instant a fault appears produces a confident explanation
    of nothing at all, so wait and re-check first.

    But only if it is coming. A fault that broke months ago is never going
    to publish its text, and telling someone to wait for it is worse than
    saying plainly that no cause was ever recorded.
    """
    app = state["app"]
    errors = st.get_errors(services.striim, app)

    attempts = 0
    while (errors["error_count"] == 0
           and attempts < services.evidence_attempts
           and _text_might_still_arrive(errors)):
        attempts += 1
        services.log(f"    no error text yet — waiting "
                     f"{services.evidence_wait}s "
                     f"({attempts}/{services.evidence_attempts})")
        if services.evidence_wait:
            time.sleep(services.evidence_wait)
        services.striim.invalidate()
        errors = st.get_errors(services.striim, app)

    if errors["error_count"]:
        services.log(f"    error text: {str(errors['errors'][0])[:120]}")
    elif errors.get("cluster_publishes_errors") is False:
        # Said once, plainly, instead of four identical waiting lines for
        # text that was never coming.
        services.log("    no error text on this cluster — reasoning from the "
                     "component configuration instead")
    elif attempts:
        services.log("    no error text arrived — reasoning from the rest")

    # The baseline the GUARDRAILS need. DECIDE has to be able to refuse a
    # restart on facts, not on the model's summary of them, so these are
    # fetched here whatever the model does or does not choose to look at.
    gathered = {
        **state.get("evidence", {}),
        "errors": errors,
        "flow": st.get_flow_stats(services.striim, app),
        "checkpoints": st.get_checkpoints(services.striim, app),
        "rejected_events": st.get_rejected_events(services.striim, app),
    }

    # Describe BOTH ends, always. On the real cluster this is not an
    # optional extra - issuesList came back empty for all 576 applications,
    # 228 of which were in HALT, so there is frequently no error text at
    # all and the component configuration IS the evidence. It is also what
    # turns "check the source system" into "AI.csv is missing from
    # UploadedFiles/sarah_moolai/".
    for end in ("source", "target"):
        component = (gathered["flow"].get(end) or {}).get("component")
        if not component:
            continue
        described = st.describe_component(services.striim, component)
        gathered[f"{end}_config"] = described

        # The real adapter name beats guessing from the component name.
        # `sarah_moolai.File_Trial5_Target` resolves to nothing useful by
        # name alone; DESCRIBE says FileWriter.
        if described.get("adapter"):
            gathered["flow"][end]["profile"] = profile_for(
                component, role=end, adapter=described["adapter"])
            described["profile"] = gathered["flow"][end]["profile"]

    # Is the thing it depends on actually back? Nothing else in the agent
    # answers that, and every "should I restart it?" turns on it. Read-only,
    # five second timeout, and "could not check" is a real answer.
    # Not calling get_rejected_rows here any more. The rows are not
    # retrievable over REST - a console SELECT returns a query handle, and
    # probing the three plausible retrieval routes against the live cluster
    # found none of them. Calling it on every incident would create a
    # transient ad-hoc query application in the Global namespace and come
    # back with the statement get_rejected_events already carries.
    #
    # The tool stays available for the model to call deliberately, and it
    # will start returning rows for free if a future Striim version answers
    # differently. It is just no longer on the automatic path.

    source_config = gathered.get("source_config")
    if source_config:
        gathered["dependency"] = probes.check_dependency(source_config)
        services.log(f"    {probes.describe_reachability(gathered['dependency'])}")

    # Is the cause actually gone? This is gathered HERE, with the rest of
    # the baseline, and not as a step after the incident has been posted -
    # otherwise the parent message announces "fixing it" and then has to
    # retract it. It is also the answer every refusal so far has turned on,
    # which makes it evidence, not an action.
    gathered.update(_probe_databases(state, services, gathered))

    # Where is it actually up to, and has that moved since last time? A
    # pipeline that reports RUNNING while its position stands still is
    # invisible to everything else on the platform.
    observe(services, app, state, gathered)
    gathered["stall"] = stall(state, services, gathered)
    if gathered["stall"]["stalled"]:
        services.log(f"    STALLED: {gathered['stall']['note'][:130]}")

    return {"evidence": gathered, "notes": state.get("notes", []) +
            ([f"waited {attempts * services.evidence_wait}s for error text"]
             if attempts else [])}


def _probe_databases(state, services, gathered):
    """Ask both ends of the pipeline directly, where we are allowed to.

    Unconfigured is not a failure - it is a smaller agent. Every one of
    these says "I could not look" in words rather than returning something
    that reads like a finding.
    """
    found = {}

    # Striim already knows where the databases are - host, port, database
    # and username are all in the component configuration we just read. So
    # discover them from there rather than making an operator hand-write a
    # connection URL per pipeline. The password is the one thing Striim
    # will not give back, and the one thing we ask for.
    # Cleared first, and per incident. The supervisor watches several
    # pipelines in one process, so a connection discovered for one app must
    # never still be sitting there when the next app is diagnosed - that
    # would be the agent querying the wrong customer's database and
    # reporting the answer with confidence.
    db.forget()

    for role in ("source", "target"):
        described = gathered.get(f"{role}_config")
        if described:
            endpoint = db.remember(role, described)
            found[f"{role}_endpoint"] = endpoint.redacted()
            if endpoint.usable:
                # Say which host resolved, so the banner's "discovered per
                # pipeline" is followed by the actual answer.
                services.log(f"    {role} db: {endpoint.where()} as "
                             f"{endpoint.user} (password from "
                             f"{endpoint.password_from})")
            elif endpoint.problem:
                services.log(f"    {role} db: "
                             f"{endpoint.problem.splitlines()[0][:110]}")

    found["target_db"] = db.db_probe("target")
    if found["target_db"].get("configured"):
        services.log(f"    target database: {found['target_db']['note'][:100]}")

    # The table the writer is configured to write to. If it has been
    # renamed or dropped, that is the fault, and it can be named.
    table = _target_table(gathered)
    if table and found["target_db"].get("reachable"):
        found["target_table"] = db.db_table_exists("target", table)
        services.log(f"    {found['target_table']['note'][:100]}")

        # Can the connected user actually WRITE here? A revoked INSERT
        # leaves every other check passing - host up, table present,
        # schema right, SELECT 1 fine - and every write failing. Without
        # this the diagnosis eliminates the target and lands on the
        # source, which is untouched and blameless.
        if found["target_table"].get("exists"):
            found["target_privileges"] = db.db_check_privileges("target",
                                                                table)
            if found["target_privileges"].get("can_write") is False:
                services.log(f"    {found['target_privileges']['note'][:120]}")

        # The table existing is not the schema matching, and this runs
        # UNCONDITIONALLY once the table is there - not only when the
        # source table happens to be resolvable.
        #
        # It was gated on that before, and the gate is what produced a
        # wrong answer live: a column had been dropped from the target, the
        # comparison quietly did not happen, no `schema_diff` appeared in
        # the evidence at all, and the diagnosis inferred a SOURCE fault
        # from both ends being silent. An absent check has to look
        # different from a passed one or it will be read as one.
        if found["target_table"].get("exists"):
            found["schema_diff"] = db.db_compare_schema(
                _source_table(gathered), table)
            diff = found["schema_diff"]
            if diff.get("checked") is False or not diff.get("matches", True):
                services.log(f"    schema: {diff['note'][:130]}")

    # For change capture the real position lives in the replication slot,
    # not only in Striim's checkpoint, and the two can disagree.
    behaviour = ((gathered.get("flow", {}).get("source") or {})
                 .get("profile") or {}).get("behaviour")
    if behaviour == "cdc_reader" and config.SOURCE_DB_URL:
        slot = _configured_slot(gathered)
        found["replication_slot"] = db.db_replication_slot_status(slot)
        services.log(f"    {found['replication_slot']['note'][:120]}")

    return found


def _target_table(gathered):
    """The single table the writer is configured for, if there is just one."""
    props = (gathered.get("target_config") or {}).get("properties") or {}
    lowered = {str(k).lower(): v for k, v in props.items()}
    tables = str(lowered.get("tables") or "").strip()

    if not tables or "%" in tables:
        return None            # a wildcard mapping names no single table
    names = [t.strip() for t in tables.split(",") if t.strip()]
    return names[-1] if names else None


def _source_table(gathered):
    """The single table the reader is configured for, if there is just one."""
    props = (gathered.get("source_config") or {}).get("properties") or {}
    lowered = {str(k).lower(): v for k, v in props.items()}
    tables = str(lowered.get("tables") or "").strip()

    if not tables or "%" in tables:
        return None
    names = [t.strip() for t in tables.split(",") if t.strip()]
    return names[0] if names else None


def _configured_slot(gathered):
    props = (gathered.get("source_config") or {}).get("properties") or {}
    lowered = {str(k).lower(): v for k, v in props.items()}
    return (lowered.get("replicationslotname")
            or lowered.get("replicationslot") or None)


def _text_might_still_arrive(errors):
    """Only wait when waiting could actually pay.

    Three ways it cannot, and the first is the common one:

      the cluster publishes no issue text for ANY application, so there is
      nothing pending anywhere and waiting is pure dead air
      the fault is old, so its text was never recorded
      ...otherwise, text really may still be on its way
    """
    if errors.get("cluster_publishes_errors") is False:
        return False

    age = errors.get("fault_age_seconds")
    if age is None:
        return True          # unknown age - one look back is worth it
    return age < 300


# ============================================================= APPROVAL
#
# Additive DDL runs. Destructive DDL asks, in the thread the incident is
# already in - Socket Mode is listening, so this needs no buttons and no
# public callback URL.
#
# WHY THE PENDING REPAIR IS PLAIN DATA. It has to survive a wait of
# minutes between the question and the answer, across two entirely
# different call stacks: one graph run posts it, and a Slack message
# handler picks it up. A closure would tie it to a stack frame that is
# long gone. Everything needed to run it later is a dict.


class AwaitingApproval(Exception):
    """Raised instead of running something destructive unasked.

    An exception rather than a return value on purpose: a caller cannot
    ignore it and carry on as though the change had happened.
    """

    def __init__(self, pending):
        super().__init__(pending["question"])
        self.pending = pending


def _expired_record(pending, minutes):
    """The parent, once the offer to run it has been withdrawn.

    It says the same three things the live ask said - what the statement is,
    what it touches, how to undo it - and replaces "reply approve" with the
    fact that the window closed and nothing happened.
    """
    plan = pending.get("plan") or {}
    counts = plan.get("counts") or {}
    rows, table = counts.get("rows"), plan.get("table")

    scale = ""
    if rows and table:
        scale = f" It would have affected {rows:,} rows in {table}."
    elif rows:
        scale = f" It would have affected {rows:,} rows."

    return {
        "shape": "needs_command", "did": pending.get("did") or [],
        "why_i_cannot": (
            f"I asked for approval to run this and waited {minutes} minutes. "
            f"Nobody answered, so I stopped waiting and changed nothing — no "
            f"statement was run and no backup was taken.{scale}"),
        "command": {
            "sql": pending["summary"],
            "where": "the target database",
            "derived_from": plan.get("derived_from"),
            "next": "Run it yourself, then resume the pipeline. This incident "
                    "stays open and I will not post about it again while the "
                    "pipeline sits in this state — ✅ it when it is dealt with.",
            "reversal": plan.get("restore")},
    }


def _approval_facts(pending):
    """The parts of a pending approval the PARENT message needs.

    Everything a person needs to answer with - what it touches, how much,
    where the copy goes and how to put it back - travels on the parent.
    The thread used to carry it, which meant the notification preview said
    only that something needed approving and the detail was one tap away.
    """
    plan = pending.get("plan") or {}
    return {"counts": plan.get("counts") or {},
            "table": plan.get("table"),
            "backup": pending.get("backup"),
            "restore": plan.get("restore"),
            "derived_from": plan.get("derived_from")}


def request_approval(state, services, prepared, skill_name):
    """Post the statement, the counts and the backup name, and wait."""
    thread = state.get("slack_thread_ts")
    app = state["app"]

    if services.notifier.is_closed(thread):
        raise target_repair.RepairRefused(
            "a human already marked this incident resolved, so nothing "
            "further is run against it")

    existing = services.pending_for(thread)
    if existing:
        raise AwaitingApproval(existing)

    counts = prepared.get("counts") or {}
    question = (
        f"{ddl.describe(prepared)}\n"
        f"   This one destroys something, so I am not running it unasked.\n"
        f"   Reply *approve* to run it, or *skip* to leave it alone. "
        f"I will give up in {config.APPROVAL_TIMEOUT_SECONDS // 60} minutes.")

    pending = {
        "app": app, "thread": thread, "skill": skill_name,
        "fingerprint": state.get("fingerprint"),
        "plan": prepared, "asked_at": time.time(),
        "state": "waiting", "question": question,
        "summary": prepared["statement"],
        "rows": counts.get("rows"),
        "backup": prepared.get("backup_table"),
        # Kept so the parent can be rewritten later without the graph
        # state, which is long gone by the time an approval times out.
        "verdict": state.get("verdict"),
        "did": list(state.get("did") or []),
    }
    services.pending[thread] = pending

    act.audit({"approval": "requested", "incident": state.get("fingerprint"),
               "app": app, "statement": prepared["statement"],
               "backup": prepared.get("backup_table"), "counts": counts})

    services.log(f"    awaiting approval: {prepared['statement']}")
    raise AwaitingApproval(pending)
#
# The fault nothing else on the platform can see. Observed live:
# sarah1_CDC reported RUNNING, moved nothing for 34 minutes, its
# replication slot was inactive, and ten consecutive checkpoints recorded
# the identical LSN 0/1D5F690 while their timestamps advanced.
#
# Every check the platform makes passed. The status word is RUNNING and
# the checkpoints are being written, so anything that reasons from either
# reports a healthy pipeline. What is actually true is that no data has
# moved, and the only way to see it is to look at the POSITION rather than
# at the fact that a checkpoint happened.


def position_of(ev):
    """Where the reader has actually got to, as something comparable.

    The replication slot is the authority for change capture - Striim's
    checkpoint records that a checkpoint occurred, which is a different
    claim from where it occurred. Falls back to the component's own
    recorded offset for connectors with no slot.
    """
    slot = ev.get("replication_slot") or {}
    for entry in slot.get("slots") or []:
        if entry.get("confirmed_flush_lsn"):
            return f"lsn:{entry['confirmed_flush_lsn']}"

    position = ((ev.get("source_config") or {}).get("read_position") or {})
    if position.get("offset_end_bytes") is not None:
        return f"offset:{position['offset_end_bytes']}"
    return None


def observe(services, app, state, ev):
    """Remember where it was, so next time we can tell whether it moved."""
    slot = ev.get("replication_slot") or {}
    history = services.observations.setdefault(app, [])
    history.append({
        "at": time.time(),
        "status": state.get("status"),
        "position": position_of(ev),
        "checkpoint": (ev.get("checkpoints") or {}).get("last_checkpoint_time"),
        # Both LSNs, because telling a stalled pipeline from an idle one
        # needs their movement over time, not their difference right now.
        "server_lsn": slot.get("server_lsn"),
        "flushed_lsn": slot.get("confirmed_flush_lsn"),
    })
    del history[:-10]
    return history


def work_outstanding(ev, history=None):
    """Is there anything for this pipeline to be doing? True/False/None.

    THE DIFFERENCE BETWEEN STALLED AND IDLE, and getting it wrong raised a
    false incident against a healthy pipeline. Twice.

    A stall is "there is work and it is not being done". Idle is "there is
    no work". Every symptom is identical: nothing read, nothing written,
    position unchanged, slot not streaming. A test pipeline nobody is
    inserting into shows all four and is perfectly fine.

    The first attempt at this asked whether pending_bytes was above zero,
    which is very nearly always true and so caught the healthy pipeline it
    was written to protect: an idle Postgres keeps advancing
    pg_current_wal_lsn() for checkpoints, autovacuum and the background
    writer, and a logical slot consumes none of it. The gap is not a
    backlog. Only its GROWTH is.

    So the question is asked across observations: did the server's LSN
    advance while the slot's confirmed flush stood still? That is the
    source producing work the pipeline is not taking. If neither moved,
    the source is quiet and there is nothing to be doing.

    None means the question could not be answered, and that must NOT be
    read as either - an unanswerable question is not a fault.
    """
    moved = _source_moved_while_slot_did_not(history)
    if moved is not None:
        return moved

    # Without a slot to ask, a source holding rows the target does not
    # have is the same claim by a different route.
    drift = (ev.get("reconcile") or {}).get("drift")
    if drift is not None:
        return drift > 0

    rejected = ev.get("rejected_events") or {}
    if rejected.get("has_ever_rejected") is True:
        seconds = rejected.get("seconds_since_last_rejection")
        if seconds is not None and seconds < config.STALL_SILENCE_SECONDS:
            return True          # rows arriving and being refused

    return None


def _unsure_because(state, verdict):
    """Why a restart is not being run unattended, in the reader's terms.

    Names the specific unknown where there is one, because "I am not sure"
    is only useful to somebody who is told what about.
    """
    missing = (verdict.get("missing_information") or "").strip().rstrip(".")
    if missing and not _is_a_lookup(missing):
        return (f"restarting looks right, but {missing[0].lower()}{missing[1:]}"
                f" — so this needs a person to say yes")

    cause = (verdict.get("likely_cause") or "").strip().rstrip(".")
    if cause:
        return (f"the cause looks like {cause}, but the evidence does not "
                f"settle it — restarting on a guess needs a person to say yes")

    return ("what broke this is not established well enough to restart it "
            "unattended — a person should say yes")


def _lsn_to_int(text):
    """Postgres LSNs are 'X/Y' in hex. Comparable once they are numbers."""
    try:
        high, _, low = str(text).partition("/")
        return (int(high, 16) << 32) + int(low, 16)
    except (ValueError, AttributeError):
        return None


def _source_moved_while_slot_did_not(history):
    """True / False / None, from the two LSNs across observations.

    True  - the server advanced and the slot did not: work is going undone.
    False - the server did not advance either: the source is quiet.
    None  - fewer than two readings, or the LSNs were never available.
    """
    if not history:
        return None

    readings = [(h.get("server_lsn"), h.get("flushed_lsn")) for h in history
                if h.get("server_lsn") and h.get("flushed_lsn")]
    if len(readings) < 2:
        return None

    first_server, first_flush = readings[0]
    last_server, last_flush = readings[-1]

    server = [_lsn_to_int(first_server), _lsn_to_int(last_server)]
    flush = [_lsn_to_int(first_flush), _lsn_to_int(last_flush)]
    if None in server or None in flush:
        return None

    server_advanced = server[1] > server[0]
    slot_advanced = flush[1] > flush[0]

    if not server_advanced:
        return False            # a quiet source has produced nothing
    return not slot_advanced    # it produced, and the slot did not follow


def stall_signals(state, services, ev):
    """Every independent reason to think this is moving nothing.

    Gated on there being something to move. Each signal below is also a
    symptom of a completely healthy pipeline with a quiet source, which is
    why none of them counts on its own.
    """
    signals = []
    status = state.get("status")
    running = status in ("RUNNING", "DEPLOYED")

    # No work outstanding, or no way to tell: silence is silence. This is
    # the guard whose absence turned an idle test pipeline into an
    # incident at half past four in the afternoon.
    history = services.observations.get(state["app"]) or []
    if work_outstanding(ev, history) is not True:
        return []

    # 1. The slot says nothing is consuming it, while the app says it is
    #    running. Both cannot be true.
    slot = ev.get("replication_slot") or {}
    if running and slot.get("inactive"):
        signals.append({
            "signal": "slot_inactive",
            "detail": f"replication slot {', '.join(slot['inactive'])} is "
                      f"inactive while the application reports {status} — "
                      f"nothing is reading it"})

    # 2. The position has not moved across several looks, while the
    #    checkpoints kept being written. This is the ten-identical-LSNs
    #    case, seen from our own observations rather than from a console
    #    history whose shape is not verified.
    positioned = [h for h in history if h.get("position")]
    if running and len(positioned) >= config.STALL_OBSERVATIONS:
        recent = positioned[-config.STALL_OBSERVATIONS:]
        positions = {h["position"] for h in recent}
        checkpoints = {h.get("checkpoint") for h in recent}
        if len(positions) == 1 and len(checkpoints) > 1:
            signals.append({
                "signal": "position_frozen",
                "detail": f"the read position has been {recent[0]['position']} "
                          f"for {len(recent)} consecutive checks while the "
                          f"checkpoint timestamps advanced — it is "
                          f"checkpointing, not reading"})

    # 3. Both ends quiet for longer than this kind of connector goes quiet.
    flow = ev.get("flow") or {}
    source, target = flow.get("source") or {}, flow.get("target") or {}
    profile = source.get("profile") or {}
    if running and profile.get("silence_is_normal") is False:
        quiet = [n for n in (source.get("seconds_since_last_event"),
                             target.get("seconds_since_last_write"))
                 if n is not None]
        if quiet and min(quiet) > config.STALL_SILENCE_SECONDS:
            signals.append({
                "signal": "both_ends_silent",
                "detail": f"nothing has moved at either end for "
                          f"{min(quiet) // 60} minutes, and {profile.get('why')}"})

    return signals


def is_stalled(state):
    """Has the evidence already established a stall? For the guardrails."""
    return bool(((state.get("evidence") or {}).get("stall") or {})
                .get("stalled"))


def stall(state, services, ev):
    """Is this pipeline stalled? A fault, whatever the status word says."""
    signals = stall_signals(state, services, ev)
    if not signals:
        return {"stalled": False, "signals": []}

    pending = (ev.get("replication_slot") or {}).get("pending_bytes")
    return {
        "stalled": True,
        "signals": signals,
        "position": position_of(ev),
        "pending_bytes": pending,
        "note": (("; ".join(s["detail"] for s in signals))
                 + (f". {pending:,} bytes are waiting to be read"
                    if pending else "")),
        "why_it_matters": "the platform reports this pipeline as healthy. "
                          "Nothing about its status, its checkpoints or its "
                          "alerting will ever say otherwise, and data is "
                          "not moving.",
    }


# ============================================================= DIAGNOSE

def diagnose(state, services):
    """The only node that uses the model.

    Nobody decides in advance what evidence matters. The model is given the
    tools and the baseline the watcher already collected, and it decides
    what else to look at and in what order. That is why a failure mode
    nobody anticipated does not need new code - it needs the model to ask a
    different question.
    """
    app = state["app"]

    if services.model is None:
        return {"verdict": None,
                "verdict_problem": "no model is configured (ANTHROPIC_API_KEY "
                                   "is not set), so nothing was diagnosed"}

    opening = (
        f"Pipeline {app} is in {state.get('status')} and needs investigating. "
        f"Work out what is wrong and whether it is safe to restart.\n\n"
        f"The watcher already collected this before waking you. Use it as a "
        f"starting point, not as the whole picture - call whatever else you "
        f"need:\n\n"
        + json.dumps(state.get("evidence", {}), default=str)[:12000]
    )

    messages = [{"role": "user", "content": opening}]
    trace = list(state.get("trace", []))
    learned = {}          # what the model found that the watcher had not
    resolved_once = False # conditionals are settled at most one round

    for round_number in range(1, config.MAX_ROUNDS + 1):
        try:
            response = services.model.messages.create(
                model=config.MODEL,
                max_tokens=config.MAX_TOKENS,
                system=prompts.system_prompt(),
                tools=prompts.TOOL_SCHEMAS,
                messages=messages,
            )
        except Exception as error:
            # The model being unreachable is an operational fact, not a
            # crash. It becomes "a human should look at this".
            return {"verdict": None, "trace": trace, "rounds": round_number,
                    "verdict_problem": f"the model could not be reached "
                                       f"({type(error).__name__})"}

        messages.append({"role": "assistant", "content": response.content})

        blocks = _block_kinds(response.content)
        services.log(f"    [{round_number}] stop_reason={response.stop_reason} "
                     f"blocks={blocks} payload={_payload_chars(messages):,} chars")

        if response.stop_reason != "tool_use":
            text = "".join(block.text for block in response.content
                           if getattr(block, "type", None) == "text")
            verdict, problem = prompts.parse_verdict(text)

            if verdict:
                # A verdict that hedges about something a tool settles is
                # not finished. Go and settle it, then ask again - once.
                hedges = (prompts.unresolved_conditionals(
                    verdict, {**(state.get("evidence") or {}), **learned})
                    if not resolved_once else [])

                if hedges:
                    answers = _settle(hedges, state, services, learned, trace)
                    messages.append({"role": "user", "content": answers})
                    resolved_once = True
                    continue

                return {"verdict": verdict, "verdict_problem": None,
                        "trace": trace, "rounds": round_number,
                        "fault_zone": verdict.get("fault_zone", "unknown"),
                        **_merged(state, learned)}

            # The investigation was good and the ANSWER was lost. Observed
            # live: five rounds, ten well-chosen tool calls, and then an
            # empty text block - the model spent its output budget on
            # thinking, or stopped at max_tokens before writing the JSON.
            #
            # Throwing that away and reporting "could not work out what is
            # wrong" is the worst possible response, because everything
            # needed to say something useful has already been gathered.
            services.log(f"    model output unusable: {problem} "
                         f"(stop_reason={response.stop_reason})")

            verdict, retry_problem = _ask_for_the_verdict_only(
                services, messages, round_number)
            if verdict:
                return {"verdict": verdict, "verdict_problem": None,
                        "trace": trace, "rounds": round_number + 1,
                        "fault_zone": verdict.get("fault_zone", "unknown"),
                        **_merged(state, learned)}

            return {"verdict": None, "trace": trace, "rounds": round_number + 1,
                    "verdict_problem": f"{problem}; asked again for the "
                                       f"verdict alone and {retry_problem}",
                    **_merged(state, learned)}

        results = []
        for block in response.content:
            if getattr(block, "type", None) != "tool_use":
                continue

            args = dict(block.input)
            shown = (args.get("app") or args.get("component")
                     or args.get("table") or args.get("which")
                     or args.get("namespace") or "")
            services.log(f"    [{round_number}] {block.name}({shown})")
            trace.append(f"{block.name}({shown})")

            try:
                output = toolbox.call(block.name, services.striim, **args)
            except KeyError:
                output = {"error": f"no such tool: {block.name}"}
            except Exception as error:
                # A failing tool must not end the investigation. Tell the
                # model what went wrong and let it try something else.
                output = {"error": f"{type(error).__name__}: {error}"}

            # Anything the MODEL discovers has to reach the evidence the
            # GUARDRAILS read, or the two end up looking at different
            # worlds. Observed live, and it produced a message that
            # contradicted itself inside four lines: the verdict said
            # "schema diff shows source has 'age', target does not" while
            # the repair skill said "there is no schema difference". Both
            # were reporting honestly - the model had called
            # db_compare_schema itself, and the result went into the
            # conversation and nowhere else.
            key = _evidence_key(block.name, args)
            if key and not isinstance(output, str) and "error" not in output:
                learned[key] = output

            results.append({"type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(output, default=str)[:6000]})

        messages.append({"role": "user", "content": results})
        _trim(messages)

    return {"verdict": None, "trace": trace, "rounds": config.MAX_ROUNDS,
            **_merged(state, learned),
            "verdict_problem": f"the investigation did not conclude within "
                               f"{config.MAX_ROUNDS} rounds"}


# Tool results the guardrails also need to see. The model is free to call
# anything; these are the ones whose answers the CODE reasons about, so
# they are folded back into the evidence rather than living only in the
# conversation. Everything else the model calls is its own business.
_EVIDENCE_KEYS = {
    "db_compare_schema": "schema_diff",
    "db_replication_slot_status": "replication_slot",
    "get_rejected_events": "rejected_events",
    "get_checkpoints": "checkpoints",
    "get_flow_stats": "flow",
    "get_errors": "errors",
    "get_state_history": "history",
    "preflight": "preflight",
}


def _settle(hedges, state, services, learned, trace):
    """Answer the questions the verdict left open, then insist on a rewrite.

    The tools are called HERE rather than being suggested to the model,
    because the model has already had its chance to call them and wrote a
    conditional instead. This removes the option of hedging again: the
    answer is in the transcript before it is asked to try a second time.
    """
    evidence = {**(state.get("evidence") or {}), **learned}
    lines = ["You wrote a conditional about something you can check. I have "
             "checked it for you. Answer again with the JSON verdict only, "
             "stating what IS true rather than what might be:"]

    for hedge in hedges:
        services.log(f"    hedged on {hedge['question']} — calling "
                     f"{hedge['tool']}")
        args = _arguments_for(hedge["tool"], state, evidence)
        try:
            answer = toolbox.call(hedge["tool"], services.striim, **args)
        except Exception as error:
            answer = {"error": f"{type(error).__name__}: {error}"}

        trace.append(f"{hedge['tool']}(settling a conditional)")
        key = _evidence_key(hedge["tool"], args)
        if key and isinstance(answer, dict) and "error" not in answer:
            learned[key] = answer

        lines.append(f"\nYou said: \"{hedge['text']}\"")
        lines.append(f"{hedge['question'].capitalize()}: "
                     f"{json.dumps(answer, default=str)[:2500]}")

    lines.append("\nNo sentence in your answer may begin with 'if' about "
                 "anything above. Say what is the case.")
    return "\n".join(lines)


def _arguments_for(tool, state, evidence):
    """Best arguments for a tool being called to settle a conditional."""
    app = state["app"]
    flow = evidence.get("flow") or {}
    source_table = _source_table(evidence)
    target_table = _target_table(evidence)

    if tool in ("db_compare_schema", "db_compare_constraints", "reconcile"):
        return {"source_table": source_table or "public.source",
                "target_table": target_table or "public.target"}
    if tool == "db_table_exists":
        return {"which": "target", "table": target_table or "public.target"}
    if tool == "db_check_privileges":
        return {"which": "target", "table": target_table or "public.target"}
    if tool == "db_replication_slot_status":
        return {"slot": _configured_slot(evidence)}
    if tool == "describe_component":
        return {"component": (flow.get("source") or {}).get("component") or app}
    return {"app": app}


def _evidence_key(tool, args):
    """Where a tool's answer belongs in the evidence, or None.

    Two tools answer about either end, so the key depends on which end was
    asked about - putting a source probe under `target_db` would be worse
    than not recording it at all.
    """
    which = str(args.get("which") or "").lower()

    if tool == "db_probe":
        return "source_db" if which.startswith("s") else "target_db"
    if tool == "db_table_exists":
        return "source_table_exists" if which.startswith("s") else "target_table"
    if tool == "describe_component":
        return None          # role is not knowable from the arguments alone

    return _EVIDENCE_KEYS.get(tool)


def _merged(state, learned):
    """The watcher's baseline plus whatever the model went and found."""
    if not learned:
        return {}
    return {"evidence": {**(state.get("evidence") or {}), **learned}}


def _block_kinds(content):
    """text/tool_use/thinking counts, for the log. Which one was empty matters."""
    counts = {}
    for block in content or []:
        kind = getattr(block, "type", "?")
        counts[kind] = counts.get(kind, 0) + 1
    return ",".join(f"{k}x{v}" for k, v in sorted(counts.items())) or "none"


def _payload_chars(messages):
    return len(json.dumps(messages, default=str))


def _trim(messages, keep_full=3, shrink_to=1200):
    """Shrink older tool results in place as the conversation grows.

    Ten tool results at 6,000 characters each is 60,000 characters of
    history before the model has written a single word of its answer, and
    the most recent two or three are the ones it is actually reasoning
    about. The older ones stay - losing them entirely would change what the
    model concluded - but they are cut down.
    """
    blocks = [(i, part) for i, message in enumerate(messages)
              if isinstance(message.get("content"), list)
              for part in message["content"]
              if isinstance(part, dict) and part.get("type") == "tool_result"]

    for _, part in blocks[:-keep_full] if len(blocks) > keep_full else []:
        content = part.get("content") or ""
        if len(content) > shrink_to:
            part["content"] = (content[:shrink_to] +
                               f" …[trimmed from {len(content):,} chars; "
                               f"call the tool again if you need the rest]")


def _ask_for_the_verdict_only(services, messages, round_number):
    """One more call, with the tools taken away and the JSON asked for.

    Deliberately narrow. No new evidence is gathered and no new tool call
    is possible - `tools` is omitted entirely, so the model physically
    cannot spend this turn on another lookup. It has everything it needs;
    what it lost was the writing-down.
    """
    trimmed = list(messages)
    _trim(trimmed, keep_full=2, shrink_to=800)
    trimmed.append({"role": "user", "content":
                    "Stop investigating and answer now. Reply with ONLY the "
                    "JSON verdict described in your instructions - no tool "
                    "calls, no preamble, no explanation around it. If "
                    "something is still unknown, say so in the JSON fields "
                    "rather than looking it up."})

    services.log(f"    [{round_number + 1}] asking for the verdict alone "
                 f"({_payload_chars(trimmed):,} chars, no tools)")
    try:
        response = services.model.messages.create(
            model=config.MODEL,
            max_tokens=config.MAX_TOKENS,
            system=prompts.system_prompt(),
            messages=trimmed,
        )
    except Exception as error:
        return None, f"the model could not be reached ({type(error).__name__})"

    text = "".join(block.text for block in response.content
                   if getattr(block, "type", None) == "text")
    services.log(f"    [{round_number + 1}] stop_reason={response.stop_reason} "
                 f"blocks={_block_kinds(response.content)}")

    verdict, problem = prompts.parse_verdict(text)
    return verdict, (problem or "it answered")


def verdict_from_evidence(state):
    """Say what the tools found, when the model could not say anything.

    "The target is up, the table exists, the checkpoint is at X, rejections
    occurred N seconds ago" is a diagnosis. "Could not work out what is
    wrong" is an admission of defeat over evidence that is sitting right
    there, already gathered, already paid for.

    This is deliberately NOT clever. It reports; it does not infer a cause
    it cannot see. Confidence is always low and restart_would_help is
    always false, because the reasoning step is the part that went missing
    and no amount of formatting replaces it - the guardrails then treat it
    exactly as they treat any low-confidence verdict.
    """
    ev = state.get("evidence") or {}
    app = state.get("app", "")
    status = state.get("status")

    facts, zone = [], "unknown"

    flow = ev.get("flow") or {}
    source, target_end = flow.get("source") or {}, flow.get("target") or {}

    target_db = ev.get("target_db") or {}
    if target_db.get("reachable") is True:
        facts.append("the target database is up")
    elif target_db.get("reachable") is False:
        facts.append("the target database is refusing connections")
        zone = "target"

    table = ev.get("target_table") or {}
    if table.get("exists") is True:
        facts.append(f"{table['table']} exists")
    elif table.get("exists") is False:
        facts.append(f"{table['table']} is missing")
        zone = "target"

    schema = ev.get("schema_diff") or {}
    if schema.get("missing_in_target"):
        missing = ", ".join(schema["missing_in_target"])
        facts.append(f"the target has no {missing} column")
        zone = "target"
    elif schema.get("checked") is False:
        facts.append("the target's columns were never compared with the "
                     "source, so a dropped column would not have been seen")

    slot = ev.get("replication_slot") or {}
    if slot.get("exists") is False:
        facts.append("the replication slot is gone")
        zone = "source"
    elif slot.get("inactive"):
        facts.append(f"replication slot {slot['inactive'][0]} is inactive")
        zone = "source"

    rejected = ev.get("rejected_events") or {}
    if rejected.get("has_ever_rejected") is True:
        age = rejected.get("seconds_since_last_rejection")
        facts.append(f"the target rejected rows"
                     + (f" {age}s ago" if age is not None else ""))
        zone = "target" if zone == "unknown" else zone

    checkpoints = ev.get("checkpoints") or {}
    if checkpoints.get("last_checkpoint_time"):
        facts.append(f"the last checkpoint was "
                     f"{checkpoints['last_checkpoint_time']}")

    position = ((ev.get("source_config") or {}).get("read_position") or {})
    if position.get("offset_end_bytes"):
        facts.append(f"the reader is at byte "
                     f"{position['offset_end_bytes']:,} of "
                     f"{position.get('source_name')}")

    errors = (ev.get("errors") or {}).get("errors") or []
    if errors:
        facts.append(f"the recorded error is: {str(errors[0])[:160]}")

    if not facts:
        return None

    return {
        "healthy": False,
        "headline": f"{short_name(app)} is in {status}",
        "what_happened": _plain_list(facts) + ".",
        "likely_cause": "unknown — the evidence is below, the reasoning "
                        "step did not complete",
        "fault_zone": zone,
        "confidence": "low",
        "evidence": _plain_list(facts[:3]),
        "restart_would_help": "unknown",
        "why_not": "",
        "human_action_needed": "",
        "target_sql": "",
        "suggested_skill": None,
        "suggested_tier": 3,
        "missing_information": "the diagnosis itself — this was assembled "
                               "from the tool results, not reasoned",
        "_composed_by_code": True,
    }


def _plain_list(items):
    items = [str(i).strip().rstrip(".") for i in items if str(i).strip()]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " and " + items[-1]


# =============================================================== DECIDE

def decide(state, services):
    """The guardrails. Code has the final say, every time.

    The order is deliberate: the hard limits run BEFORE the model's opinion
    is consulted, so the model can only ever talk its way DOWN to a lower
    tier. There is no sentence it can produce that raises one.

    Tiers:  1 the agent acts    2 a human approves first    3 escalate only
    """
    app = state["app"]
    verdict = state.get("verdict")
    ev = state.get("evidence", {})

    # --- no usable verdict -------------------------------------------- #
    # Never a guess about the CAUSE - but never throw the evidence away
    # either. If the tools found things and only the reasoning went
    # missing, report what they found. Everything downstream treats this
    # exactly as it treats any other low-confidence verdict, so it can
    # reach a human but it can never reach an action.
    if verdict is None:
        composed = verdict_from_evidence(state)
        if composed:
            services.log("    composing the incident from the tool results")
            verdict = composed
            state = {**state, "verdict": composed}
            ev = state.get("evidence", {})
        else:
            return {"tier": 2, "fault_zone": "unknown",
                    "reason": state.get("verdict_problem")
                              or "could not reach a conclusion"}

    zone = verdict.get("fault_zone", "unknown")

    # --- the model says it is fine ------------------------------------ #
    if verdict.get("healthy") is True:
        # ...but a stalled pipeline looks fine from every angle the model
        # was given. RUNNING, checkpointing, no errors. The code has
        # measured that nothing moved, and that outranks an impression.
        if is_stalled(state):
            verdict = {**verdict, "healthy": False}
            state = {**state, "verdict": verdict}
        elif state.get("status") in st.FAULT_STATES:
            # Striim says broken, the model says fine. Trust the control
            # plane and hand it to a person - this disagreement is exactly
            # the kind of thing worth a human's thirty seconds.
            return {"tier": 2, "fault_zone": zone,
                    "reason": f"the diagnosis found no problem but Striim "
                              f"reports {state['status']}"}
        return {"tier": None, "silent": True, "fault_zone": zone,
                "reason": "no fault found"}

    # --- a stall is repaired in the PIPELINE, wherever it points -------- #
    #
    # Observed live, and it stopped a repair dead. sarah1_CDC stalled with
    # an inactive replication slot; the diagnosis called that a source
    # fault - a fair reading, the slot lives on the source - and the
    # source-zone rule then refused to act, so wait_and_resume was never
    # consulted at all.
    #
    # But the remedy for a stall is restarting the READER, which is a
    # pipeline action. A slot being on the source does not make restarting
    # our own pipeline a write to somebody else's database.
    #
    # This runs BEFORE the skill is chosen, because the skill layer checks
    # the zone first and would otherwise drop a pipeline-zone repair for
    # being offered on a source-zone fault. Nothing downstream is
    # weakened: if the slot is GONE rather than idle, cause_still_present
    # stops it; if a restart would re-read, restart_risk stops it.
    if zone in ("source", "unknown") and is_stalled(state):
        services.log("    stalled — the repair is restarting the reader, "
                     "which is a pipeline action")
        zone = "pipeline"
        verdict = {**verdict, "fault_zone": zone}
        state = {**state, "verdict": verdict}

    # --- a quiet pipeline is not an incident --------------------------- #
    #
    # THE CODE DECIDES WHETHER THERE IS A FAULT, not the model's
    # impression of one. A pipeline whose status is calm, which is not
    # stalled, and about which nothing concrete has been measured, has
    # nothing wrong with it that anybody can point at - and posting about
    # it is exactly the "cries wolf" failure that gets a channel muted.
    #
    # This exists because an idle test pipeline raised an incident. The
    # source was quiet, so nothing moved, so the model concluded something
    # was wrong. Nothing was.
    if (state.get("status") not in st.FAULT_STATES
            and not is_stalled(state)
            and not _cause_is_named(ev)):
        services.log(f"    {state.get('status')} with nothing measurably "
                     f"wrong — staying quiet")
        return {"tier": None, "silent": True, "fault_zone": zone,
                "reason": f"{state.get('status')}, and nothing concrete is "
                          f"wrong — a quiet source is not a fault"}

    mark = fingerprint(app, verdict, state.get("status"))

    # The skill layer needs to know how often this exact fault has been seen
    # before - one occurrence is a blip, two is a pattern worth naming.
    state = {**state, "fingerprint": mark,
             "fingerprint_seen": services.ledger.times_seen(mark)}

    # --- hard limits, in code ----------------------------------------- #
    if not app.startswith(config.NAMESPACE + "."):
        return {"tier": 3, "fingerprint": mark, "fault_zone": zone,
                "reason": f"{app} is outside {config.NAMESPACE} — diagnosed "
                          f"in full, but this agent only acts on its own "
                          f"namespace"}

    # The skill is chosen BEFORE the budget and repeat gates, because a
    # skill that will not run still produces a plan, and a plan is the most
    # useful thing an escalation can carry. "Here is the statement, and it
    # would have touched 412 rows" beats "needs a human" every time.
    skill, plan, dropped = skill_layer.choose(state, verdict.get("suggested_skill"))
    carry = {"fingerprint": mark, "fault_zone": zone,
             "skill": skill.name if skill else None,
             "skill_plan": plan.as_dict() if plan else None,
             "skill_dropped": dropped}

    # A verdict assembled from the tool results has to travel with the
    # state, or ESCALATE composes the message from the None it started with
    # and every fact gathered is lost at the last step.
    if verdict.get("_composed_by_code"):
        carry["verdict"] = verdict

    if services.ledger.already_tried(mark):
        return {**carry, "tier": 3,
                "reason": "this exact fault has already been acted on once "
                          "and came back — a repeat is a pattern, not a blip"}

    if services.ledger.budget_left(app) <= 0:
        return {**carry, "tier": 3,
                "reason": f"restart budget spent "
                          f"({config.MAX_RESTARTS_PER_HOUR} in the last hour) "
                          f"— restarting is not fixing this"}

    # --- zone rights --------------------------------------------------- #
    # The source is someone else's system of record. A wrong write there is
    # unrecoverable and invisible to us, so "unknown" is treated as "source"
    # rather than as "probably fine".
    if zone in ("source", "unknown"):
        # A source verdict reached without the target's schema having been
        # compared is the shape of the wrong answer we already got: both
        # ends quiet, the table present, nothing having looked at its
        # columns, and "source" concluded by elimination.
        #
        # It is only worth saying when the cause is genuinely UNKNOWN. On
        # an incident that already names its cause it is noise, and it
        # contradicts the finding sitting next to it.
        unchecked = None if _cause_is_named(ev) else _schema_not_compared(ev)
        return {**carry, "tier": 3,
                "reason": (unchecked
                           or verdict.get("why_not")
                           or ("the fault is in the source, which this agent "
                               "never writes to"
                               if zone == "source" else
                               "the fault could not be placed, so it is "
                               "treated as a source fault"))}

    if zone == "target":
        return _decide_target(state, services, verdict, carry)

    # --- has the cause actually cleared? ------------------------------- #
    # Asked of the databases directly, not of Striim's opinion of them.
    # This is what every previous refusal turned on.
    still_broken = cause_still_present(ev)
    if still_broken:
        return {**carry, "tier": 3, "reason": still_broken}

    # --- restart risk, checked in code not prose ----------------------- #
    risk = restart_risk(ev)
    if risk:
        return {**carry, "tier": 3, "reason": risk}

    # --- now, and only now, the model's opinion ------------------------ #
    if verdict.get("restart_would_help") is not True:
        return {**carry, "tier": 3,
                "reason": verdict.get("why_not") or "a restart would not help"}

    if verdict.get("confidence") != "high":
        # The reason a READER sees must be about their pipeline, not about
        # this agent's internals. "confidence is medium — a person should
        # approve this one" is the tier logic narrating itself: it tells
        # somebody nothing about what is wrong or what to do, and it went
        # out on a live incident. The confidence itself is still recorded,
        # in the terminal and the audit log, where it belongs.
        return {**carry, "tier": 2,
                "reason": _unsure_because(state, verdict),
                "why_unsure": f"confidence is {verdict.get('confidence')}"}

    # A skill caps the tier it can reach, and can only ever lower it. A
    # skill that changes what the pipeline does with data - ignoring an
    # exception, say - is capped at 2 in its own class definition, so no
    # amount of confidence anywhere can make it run unattended.
    if skill:
        tier = max(1, skill.max_tier)
        if tier > 1:
            return {**carry, "tier": tier,
                    "reason": f"{skill.name} needs a person to approve it"}
        return {**carry, "tier": 1,
                "action_taken": plan.summary,
                "reason": verdict.get("likely_cause") or "a transient failure"}

    return {**carry, "tier": 1,
            "action_taken": f"RESUME {app}",
            "reason": verdict.get("likely_cause") or "a transient failure"}


def _cause_is_named(ev):
    """Has something already been positively identified?

    The "you never compared the schemas" warning exists for a diagnosis
    reached by ELIMINATION. Where a specific cause has been measured -
    a dead slot, a missing table, a revoked privilege - it adds nothing
    and reads as a contradiction next to the finding.
    """
    if ((ev.get("stall") or {}).get("stalled")):
        return True
    if (ev.get("replication_slot") or {}).get("exists") is False:
        return True
    if (ev.get("target_table") or {}).get("exists") is False:
        return True
    if (ev.get("target_privileges") or {}).get("can_write") is False:
        return True
    if (ev.get("schema_diff") or {}).get("matches") is False:
        return True
    return bool((ev.get("errors") or {}).get("error_count"))


def _schema_not_compared(ev):
    """A sentence when the target's schema was never actually checked.

    Only fires where it would change what someone does: the target table
    is present, so an existence check has already said something
    reassuring, and the comparison that would have caught a dropped column
    did not happen.
    """
    table = ev.get("target_table") or {}
    if table.get("exists") is not True:
        return None

    diff = ev.get("schema_diff") or {}
    if diff.get("checked") is not False:
        return None

    return (f"{table.get('table')} exists but its columns were never "
            f"compared with the source, so a dropped or renamed column "
            f"would not have been seen — do not treat the target as ruled "
            f"out")


def cause_still_present(ev):
    """Is the thing that broke it still broken? A sentence, or None.

    Only ever answers from something it actually checked. An unconfigured
    database, or one it could not reach, produces None here - "I could not
    look" must not become "the cause has cleared", and it must not become
    "the cause is still there" either. It simply is not evidence, and the
    restart-risk rules below still apply on top of whatever this says.
    """
    target_db = ev.get("target_db") or {}
    if target_db.get("configured") and target_db.get("reachable") is False:
        return (f"the target database is still refusing connections — "
                f"{_first_clause(target_db.get('error'))}")

    table = ev.get("target_table") or {}
    if table.get("exists") is False:
        return (f"the target table {table['table']} does not exist — the "
                f"writer has nothing to write to")

    # The table can exist, match, and still refuse every write. This is
    # the one where EVERY check passes and the pipeline is still broken -
    # so it is checked before the schema, because a permissions failure
    # explains a rejected write more completely than a schema one does.
    grants = ev.get("target_privileges") or {}
    if grants.get("can_write") is False:
        missing = ", ".join(grants.get("missing") or [])
        return (f"the connected user has no {missing} permission on "
                f"{grants.get('table')} — every write fails on permissions, "
                f"and nothing about the host, the table or the schema will "
                f"show it")

    # The table can exist and still have nowhere to put the data. This is
    # the one an existence check walks straight past.
    schema = ev.get("schema_diff") or {}
    if schema.get("missing_in_target"):
        missing = ", ".join(schema["missing_in_target"])
        return (f"the target is missing the {missing} column, so every write "
                f"carrying it fails — the table exists but cannot accept "
                f"what the pipeline is sending")

    # A column the target HAS and the source never sends. Observed live on
    # sarah1_CDC: region was NOT NULL with no default, every write was
    # rejected, and this function said the cause had cleared - because it
    # only ever looked at what was MISSING from the target. The agent then
    # restarted a pipeline into the same wall and reported that the restart
    # did not hold, which was true and useless.
    blocking = schema.get("blocking_in_target") or []
    if blocking:
        names = ", ".join(str(b.get("column")) for b in blocking)
        return (f"{names} on the target is NOT NULL with no default and the "
                f"source sends no such column, so every write is still "
                f"rejected — restarting cannot change that")

    # A target column too narrow for what the source sends fails the same
    # way, on every row, for as long as it stays narrow.
    narrow = [d for d in (schema.get("differing_types") or [])
              if d.get("widening")]
    if narrow:
        first = narrow[0]
        return (f"the target's {first.get('column')} is "
                f"{first.get('target_type')} and the source sends "
                f"{first.get('source_type')}, so writes carrying it are still "
                f"rejected — restarting cannot change that")

    slot = ev.get("replication_slot") or {}
    if slot.get("configured") and slot.get("exists") is False:
        return ("the replication slot the pipeline reads through is gone, so "
                "there is no position to resume from — this needs a fresh "
                "initial load, not a restart")

    return None


def _first_clause(text):
    if not text:
        return "no detail was returned"
    return str(text).strip().splitlines()[0][:120]


def restart_risk(ev):
    """Would restarting this damage the target, silently?

    Checked HERE, in code, rather than left to the diagnosis, because it is
    the single most expensive thing to get wrong: nothing alerts on a
    duplicated table or a missing hour, the pipeline looks perfectly healthy
    afterwards, and the damage is only found when somebody queries a total.

    Two different failures, depending on what the source is:

      re-read connectors   a restart with no trustworthy position re-reads
                           from the beginning and APPENDS it all again
      streaming connectors a restart with no stored position resumes from
                           NOW and silently skips whatever changed while it
                           was down - a gap, not a duplicate

    Returns a sentence when a restart is unsafe, or None.
    """
    source = (ev.get("flow") or {}).get("source") or {}
    behaviour = (source.get("profile") or {}).get("behaviour")
    checkpoints = ev.get("checkpoints") or {}

    # Every sentence returned from here is written to be READ ON A PHONE, in
    # the dark, as the second line of an incident - short, plain, and about
    # consequences rather than mechanism. The mechanism goes in the thread.

    # Change capture and stream readers cannot re-read - they can only miss.
    if behaviour in RESUMABLE_BEHAVIOURS:
        if not checkpoints.get("has_usable_checkpoint"):
            return ("a restart resumes from now and silently skips "
                    "everything missed")
        return None

    if behaviour not in RE_READ_BEHAVIOURS:
        return None

    if not checkpoints.get("has_usable_checkpoint"):
        return ("a restart re-reads the whole source and duplicates every "
                "row — nothing is saved to resume from")

    position = (ev.get("source_config") or {}).get("read_position") or {}
    offset = position.get("offset_end_bytes")
    full_path = (ev.get("source_config") or {}).get("expects")
    named = _basename(full_path) or position.get("source_name") or "the source"

    if position.get("offset_looks_plausible") is False:
        return (f"the saved position does not match {named}, so a restart "
                f"re-reads it all and duplicates every row")

    if not offset:
        # Nothing has been read past the start, so there is nothing that
        # could be read a second time.
        return None

    # A checkpoint can exist, be non-N/A, and still be worthless. The
    # observed case: an offset of 170,969 bytes recorded against a file of
    # about a kilobyte, because the position had accumulated across
    # successive replacements of that file. Striim cannot seek to it, so it
    # starts again from the beginning and appends the lot.
    #
    # No arithmetic settles this. Striim reports the position but never the
    # size of the thing it points into, so any byte threshold that catches
    # 170,969 fires wrongly on a genuinely large file. What settles it is
    # whether the position can be shown to still refer to the data that is
    # there - and there are only two ways to show that:
    if _source_is_implicated(ev):
        return (f"the saved position points into {named}, which is the thing "
                f"that failed — a restart re-reads it all")

    if not (ev.get("errors") or {}).get("error_count"):
        # No error text at all - the normal case on the real cluster, where
        # issuesList is empty for every one of 576 applications. So nothing
        # tells us which component failed, and therefore nothing confirms
        # the position is still good. Refuse.
        #
        # This is the honest answer, not a cautious one. The alternative is
        # to restart on the assumption that silence means the source is
        # fine, and the cost of being wrong is a silently doubled target
        # that no alert will ever fire about.
        return (f"a restart re-reads {named} from byte {offset:,} and "
                f"duplicates every row")

    # There IS error text and it does not name the source, so the fault is
    # downstream: the reader is still sitting where it was, and resuming
    # picks up rather than starting again.
    return None


def _as_plan(blob):
    """Rebuild a RepairPlan from the dict carried through the graph state."""
    return skill_layer.RepairPlan(**blob)


def _normalise(text):
    """Two sentences that say the same thing, compared as one string."""
    return "".join(c for c in str(text).lower() if c.isalnum())


def _basename(path):
    """AI.csv, not UploadedFiles/sarah_moolai/AI.csv.

    The short name goes in the five-line message; the full path goes in the
    "do this" line, where somebody is about to type it.
    """
    if not path:
        return None
    return str(path).replace("\\", "/").rstrip("/").rsplit("/", 1)[-1] or None


def _source_is_implicated(ev):
    """Does the error text blame the SOURCE component?

    Deliberately literal: it looks for the source component's own name, or
    the path it is configured to read, in the recorded error text. No
    keyword guessing about what "failed" means - either Striim named the
    source or it did not.
    """
    errors = " ".join(str(e) for e in (ev.get("errors") or {}).get("errors", []))
    if not errors:
        return False

    component = ((ev.get("flow") or {}).get("source") or {}).get("component")
    expects = (ev.get("source_config") or {}).get("expects")

    needles = []
    if component:
        # Both forms - Striim writes the fully qualified name in some
        # messages and the bare component name in others.
        needles += [component, component.rsplit(".", 1)[-1]]
    if expects:
        # The configured path, and the filename on the end of it. Not the
        # extension - "csv" would match half the cluster.
        needles.append(expects)
        basename = str(expects).replace("\\", "/").rsplit("/", 1)[-1]
        if len(basename) > 3:
            needles.append(basename)

    return any(needle and needle in errors for needle in needles)


def _decide_target(state, services, verdict, carry):
    """Zone 2. Corrective SQL against the target, or a proposal for a human.

    Never a restart first: resuming into a target that is still holding a
    half-written batch just reproduces the failure, or worse, doubles it.

    A skill takes precedence over a raw SQL suggestion, because a skill has
    preconditions and a raw statement has only the model's word for it.
    """
    skill_name, plan = carry.get("skill"), carry.get("skill_plan")

    if skill_name and plan:
        # Two independent gates, and BOTH must open. The skill's own class
        # caps how far it can ever go - DDL and exception-ignoring are
        # capped at 2 no matter what - and the plan only marks itself
        # executable once it has a measured row count under the threshold.
        # Irreversibility does not block tier 1 here: a bounded, counted,
        # under-threshold delete is exactly what zone 2 exists to allow.
        skill = skill_layer.SKILLS.get(skill_name)
        cap = skill.max_tier if skill else 2
        return {**carry,
                "tier": 1 if (plan["executable"] and cap == 1) else 2,
                "action_taken": plan["summary"],
                "reason": plan["reason"] or plan["summary"]}

    mark = carry["fingerprint"]
    sql = (verdict.get("target_sql") or "").strip()
    if not sql:
        return {**carry, "tier": 2,
                "reason": carry.get("skill_dropped")
                          or verdict.get("human_action_needed")
                          or "the target needs attention and the agent has no "
                             "bounded statement that would fix it"}

    repair = target_repair.plan_repair(sql, verdict, incident=mark)
    return {**carry,
            "tier": 1 if repair["will_execute"] else 2,
            "repair_plan": repair,
            "action_taken": f"target repair on {short_name(state['app'])}",
            "reason": repair["reason"]}


# ============================================================= ESCALATE

def _next_step(state):
    """The single next action, when the diagnosis did not name one.

    Built from the component configuration rather than from a template, so
    it says "restore AI.csv to UploadedFiles/sarah_moolai/" instead of
    "investigate the source". The whole point of describe_component is that
    this line can be specific.
    """
    if state.get("tier") not in (2, 3):
        return None

    # A skill that produced a plan has already worked out the next action,
    # and worked it out from the component's own configuration. That is the
    # whole point of escalate_source_problem: "restore
    # UploadedFiles/sarah_moolai/AI.csv, then restart the pipeline" rather
    # than "check the source system".
    plan = state.get("skill_plan")
    if plan and plan.get("summary"):
        return plan["summary"]

    # A database the agent cannot open is the most specific thing it can
    # possibly say, and it was saying the least specific: it has the host,
    # the database name, the component that names it and the server's own
    # words for why it refused. See _connection_action.
    unreachable = _connection_action(state)
    if unreachable:
        return unreachable

    zone = state.get("fault_zone")
    expects = (state.get("evidence", {}).get("source_config") or {}).get("expects")

    if zone == "source" and expects:
        return f"check {expects} is there and readable, then restart by hand"
    if expects:
        return f"confirm {expects} before restarting — the agent will not"
    return "a person should look at this before anything is restarted"


def _connection_action(state):
    """What to do about a database that would not open.

    The agent discovers where to connect FROM the pipeline's own component
    configuration, so when a database is renamed, moved or has its password
    rotated, the agent inherits the stale details and fails exactly the way
    the writer does. That is not something it can repair - it has no write
    path into Striim's component configuration, and creating or renaming a
    database is outside the table-level DDL it is allowed - but it is
    something it can describe completely.
    """
    evidence = state.get("evidence") or {}
    for role in ("target", "source"):
        probe = evidence.get(f"{role}_db") or {}
        if probe.get("reachable") is not False:
            continue

        endpoint = db.endpoint_for(role)
        where = ""
        if endpoint and endpoint.host:
            where = f" on {endpoint.host}"
        named = f'"{endpoint.database}"' if endpoint and endpoint.database \
            else "the database"
        component = (f" The {role} component {endpoint.component} is the one "
                     f"that names it.") if endpoint and endpoint.component else ""

        error = str(probe.get("error") or "")
        lowered = error.lower()

        if "does not exist" in lowered:
            return (f"{named}{where} does not exist. Either rename it back, "
                    f"or point the {role} component at the new name in Striim "
                    f"and restart the pipeline. The agent cannot do either — "
                    f"it does not create databases and it cannot edit "
                    f"component configuration.{component}")
        if "password" in lowered or "authentication" in lowered:
            variable = getattr(endpoint, "password_from", "") or ""
            return (f"{named}{where} refused the password. Update "
                    f"{variable or 'the password variable for this host'} "
                    f"and restart the agent.{component}")
        if "role " in lowered:
            return (f"the user {getattr(endpoint, 'user', '')} is not "
                    f"accepted by {named}{where}. Restore it or point the "
                    f"{role} component at a user that exists.{component}")
        return (f"{named}{where} would not open: {_short_error(error)}. "
                f"Nothing can be read or repaired there until it does."
                f"{component}")
    return None


def _short_error(text):
    """The server's own first line, without the stack of a Python repr."""
    body = str(text).split(":", 1)[-1].strip()
    return body.splitlines()[0].strip() if body else str(text)


def _cause_needs_a_person(state):
    """The floor under a failed repair's next action.

    _next_step declines at tier 1, because at tier 1 the agent is the one
    acting and there is nothing to hand over. That stops being true the
    moment the repair does not hold: a parent that says "tried it, it did
    not work" and then nothing at all is exactly the message this standard
    exists to prevent. So name the cause and hand it over.
    """
    cause = (state.get("verdict") or {}).get("likely_cause") or ""
    cause = cause.strip().rstrip(".")
    if cause:
        return (f"clear the cause by hand — {cause} — then restart it. "
                f"Restarting it again on its own needs a person, not "
                f"another attempt.")
    return "this needs a person — a second restart will not do anything new."


def _why_in_full(state):
    """The longer reasoning, for the thread rather than the message."""
    evidence = state.get("evidence", {})
    position = (evidence.get("source_config") or {}).get("read_position") or {}
    checkpoints = evidence.get("checkpoints") or {}

    parts = []
    if position.get("offset_end_bytes"):
        parts.append(f"Saved read position: byte "
                     f"{position['offset_end_bytes']:,} of "
                     f"{position.get('source_name')}, recorded "
                     f"{position.get('position_created')}.")
    if checkpoints.get("last_checkpoint_time"):
        parts.append(f"Last checkpoint: {checkpoints['last_checkpoint_time']}.")
    elif checkpoints.get("recovery_enabled") is False:
        parts.append("Recovery is not enabled — every start is a cold start.")

    if (evidence.get("errors") or {}).get("cluster_publishes_errors") is False:
        parts.append("No error text: this cluster publishes none for any "
                     "application, so the cause came from the component "
                     "configuration rather than from a message.")

    return " ".join(parts) if parts else None


def _waiting_record(state, did):
    """The parent while a destructive repair is held for approval.

    Everything needed to answer is HERE: the statement, how many rows it
    touches, where the copy will be, and what to reply. Nobody should have
    to open a thread to find out what they are approving.
    """
    pending = state.get("approval") or {}
    counts = pending.get("counts") or {}
    rows = counts.get("rows")
    table = pending.get("table")

    scale = ""
    if rows and table:
        scale = f" It affects {rows:,} rows in {table}."
    elif rows:
        scale = f" It affects {rows:,} rows."

    return {"shape": "needs_command", "did": did,
            "why_i_cannot": (
                "This one destroys something, so I am not running it "
                "unasked." + scale
                + (f" I will copy them to {pending['backup']} first."
                   if pending.get("backup") else "")),
            "command": {"sql": state["awaiting_approval"],
                        "where": "the target database",
                        "derived_from": pending.get("derived_from"),
                        "next": f"Reply *approve* to run it, or *skip* to "
                                f"leave it alone. On *approve* I run it, "
                                f"resume the pipeline and verify. If nobody "
                                f"answers I will give up in "
                                f"{config.APPROVAL_TIMEOUT_SECONDS // 60} "
                                f"minutes and say so.",
                        "reversal": pending.get("restore")}}


def build_record(state):
    """The FACTS half of an incident message. Assembled from what happened.

    Nothing in here comes from the model. "I did" is read off the action
    results, "Result" off the verification, the refusal off the guardrail
    that fired. That separation is the whole reason the message can be
    trusted: the model can be wrong about the cause and the message still
    reports accurately what the agent did about it.
    """
    tier = state.get("tier")
    verification = state.get("verification") or {}
    did = list(state.get("did") or [])

    # No usable diagnosis is its own shape. It is not "not safe to restart"
    # - the agent has no opinion about that - it is "I could not tell".
    if state.get("verdict") is None:
        return {"shape": "unknown",
                "blocked_reason": state.get("reason"),
                "next_action": _human_action(state)}

    # --- is it waiting on a person? ------------------------------------ #
    #
    # BEFORE the verification block, and that ordering is the whole point.
    # A repair held for approval has not run, so any verification sitting
    # in the state is about a pipeline nobody touched. Checking recovery
    # first is what produced "Tried it, and it did not hold" on an incident
    # whose actual state was "waiting for you to reply approve" - the ask
    # was overwritten by a report of an attempt that never happened, so
    # nobody answered, so it expired.
    if state.get("awaiting_approval"):
        return _waiting_record(state, did)

    # --- has it finished, and how? ------------------------------------- #
    if state.get("closing"):
        if verification.get("recovered") is True:
            return {"shape": "fixed", "did": did,
                    "result": _result_line(state)}
        if did and verification.get("recovered") is None:
            return {"shape": "needs_command" if _command_of(state)
                    else "blocked",
                    "did": did,
                    "blocked_reason": verification.get("detail")
                                      or "the repair ran but nothing has "
                                         "moved yet, so it cannot be confirmed",
                    "next_action": _human_action(state),
                    "command": _command_of(state),
                    "why_i_cannot": _why_i_cannot(state)}
        if verification.get("recovered") is False:
            return {"shape": "failed", "did": did,
                    "blocked_reason": f"the cause is still there — "
                                      f"{verification.get('detail')}",
                    "next_action": _human_action(state) or _cause_needs_a_person(state)}

    # --- it would have acted, but the agent is not live ----------------- #
    if state.get("dry_run_statement"):
        return {"shape": "needs_command", "did": did,
                "why_i_cannot": "The agent is in dry-run mode, so it changed "
                                "nothing.",
                "command": {"sql": state["dry_run_statement"],
                            "where": "the Striim console",
                            "derived_from": state.get("reason"),
                            "next": "Set STRIIM_LIVE=1 and restart the agent "
                                    "and it will do this itself.",
                            "reversal": None}}

    # --- a literal statement for a person to run ----------------------- #
    command = _command_of(state)
    if command and tier in (2, 3):
        return {"shape": "needs_command", "command": command, "did": did,
                "why_i_cannot": _why_i_cannot(state)}

    # --- deliberately not acting --------------------------------------- #
    if tier in (2, 3):
        # "Stopped, not safe to restart" about a pipeline whose console
        # reads RUNNING makes the message argue with the screen the reader
        # is looking at.
        stalled = is_stalled(state) and state.get("status") not in (
            "HALT", "TERMINATED", "CRASH")
        return {"shape": "stalled" if stalled else "blocked", "did": did,
                "blocked_reason": state.get("reason"),
                "next_action": _human_action(state)}

    if tier is None:
        return {"shape": "unknown",
                "blocked_reason": state.get("reason"),
                "next_action": _human_action(state)}

    # --- about to act -------------------------------------------------- #
    return {"shape": "working", "plan_line": _plan_line(state)}


def _result_line(state):
    """What actually moved. Numbers where there are numbers."""
    verification = state.get("verification") or {}
    parts = []

    # A backfill's own result comes first, because it is the one number
    # that says whether the DATA is right - as opposed to whether the
    # pipeline is running, which is a different and easier question.
    filled = (state.get("action_result") or {}).get("backfill") or {}
    if filled.get("keys_missing_after") is not None:
        if filled["keys_missing_after"] == 0:
            parts.append(f"source and target both at "
                         f"{filled.get('target_rows_after', 0):,} rows")
        else:
            parts.append(f"{filled['keys_missing_after']:,} rows still "
                         f"missing from the target")
    elif filled.get("nulls_after") is not None:
        column = filled.get("column", "the column")
        parts.append(f"{filled['nulls_after']:,} rows left with a null "
                     f"{column}")

    delta = state.get("rows_written")
    if delta:
        parts.append(f"{delta:,} rows written")

    detail = verification.get("detail")
    if detail:
        parts.append(detail)

    status = ((verification.get("after") or {}).get("status") or "").lower()
    if status:
        parts.append(f"{status} normally" if status == "running" else status)

    return ", ".join(parts) if parts else "recovered"


def _command_of(state):
    """The literal statement a human must run, if there is one.

    Never a description. TASK-4's rule and this project's: if the agent
    cannot execute something, it hands over the finished statement, says
    where to run it, and says what happens next.
    """
    plan = state.get("skill_plan") or {}

    # `command` is set only when there genuinely is a statement to run. A
    # skill whose next step is a physical action - restore a file, recreate
    # a slot - leaves it empty, and the message uses plain English instead.
    # Sniffing the statements list for something that looks runnable turned
    # a source escalation into "run this", which is worse than useless.
    if plan.get("command"):
        return {
            "sql": plan["command"],
            "where": plan.get("run_on") or "the target database",
            "derived_from": plan.get("derived_from"),
            "next": ("I will resume the pipeline and verify as soon as it "
                     "is done."),
            "reversal": plan.get("reversal"),
        }

    repair = state.get("repair_plan") or {}
    if repair.get("sql") and not repair.get("will_execute"):
        return {"sql": repair["sql"], "where": "the target database",
                "derived_from": None,
                "next": "I will resume and verify once it is done.",
                "reversal": None}
    return None


def _why_i_cannot(state):
    plan = state.get("skill_plan") or {}
    reason = plan.get("reason") or (state.get("repair_plan") or {}).get("reason")
    if not reason:
        return None
    return f"Everything else is ready. {reason[0].upper()}{reason[1:]}."


def _plan_line(state):
    plan = state.get("skill_plan") or {}
    if plan.get("summary"):
        return f"About to {plan['summary']}."
    return None


def _human_action(state):
    """The single next action. Computed, never the model's lookup request."""
    plan = state.get("skill_plan") or {}
    if plan.get("summary") and plan.get("executable") is False:
        return plan["summary"]

    asked = (state.get("verdict") or {}).get("human_action_needed") or ""
    if asked.strip() and not _is_a_lookup(asked) and not _contradicts(state, asked):
        return asked.strip()

    return _next_step(state)


_RESTART_WORDS = ("restart", "resume", "start the", "bounce", "recycle",
                  "redeploy", "re-start")


def _contradicts(state, action):
    """Does this action tell a human to do the thing the agent refused?

    Seen live: the incident declined to restart, then said "Restart the
    source component to reconnect to striim_slot". Both cannot be right.
    Either restarting is safe - in which case the agent should have done
    it, or said which gate stopped it - or it is not, and instructing
    somebody else to do it is worse than saying nothing, because they will
    do it and it will not be recorded anywhere.
    """
    if not _refused_a_restart(state):
        return False
    lowered = f" {action.strip().lower()} "
    return any(word in lowered for word in _RESTART_WORDS)


def _refused_a_restart(state):
    """Did this incident decline to restart, for any reason?"""
    if state.get("did"):
        return False        # it acted; nothing was refused
    reason = f"{state.get('reason') or ''}".lower()
    if "not restart" in reason or "restarting cannot" in reason:
        return True
    verdict = state.get("verdict") or {}
    return (verdict.get("restart_would_help") is False
            and state.get("tier") in (2, 3))


_LOOKUP_WORDS = ("inspect", "check ", "look at", "review", "investigate",
                 "examine", "find out", "determine", "verify whether",
                 "see what", "identify the")


def _is_a_lookup(text):
    """Is this asking a human to go and read something?

    That is the fault TASK-4 opens with. The agent read the exception
    store; it must not then tell somebody else to. Anything matching here
    is dropped and replaced by a computed action.
    """
    lowered = f" {str(text).strip().lower()} "
    return any(word in lowered for word in _LOOKUP_WORDS)


def escalate(state, services):
    """Write the incident and post it.

    Runs on every path that has something to say, not only on the ones the
    agent cannot fix - because the thread has to EXIST before the action and
    the verification can be replies to it. Whoever scrolls in tomorrow reads
    the incident from the top: what broke, what was tried, what happened.
    """
    app = state["app"]
    verdict = state.get("verdict")
    tier = state.get("tier")
    reason = state.get("reason")

    text = slack_io.compose_incident(app, verdict, build_record(state))
    thread = services.notifier.post_incident(text)

    # NOTHING ELSE GOES TO SLACK HERE.
    #
    # This used to post four or five more messages underneath: the
    # confidence and the zone and the tool count, the read position and
    # the last checkpoint, a note about the cluster publishing no error
    # text, the repair plan, and "considered X but declined". None of them
    # is a state change. All of them were explanations of a decision the
    # parent had already stated, and together they turned every incident
    # into a wall nobody read.
    #
    # It all still exists - in the terminal, where somebody debugging the
    # agent is looking anyway, and in the audit log, which is permanent.
    # Slack gets the decision.
    for detail in (slack_io.compose_detail(state), _why_in_full(state),
                   state.get("skill_dropped")):
        if detail:
            services.log(f"    {detail}")

    act.audit({"incident": state.get("fingerprint"), "app": app,
               "tier": tier, "reason": reason,
               "zone": state.get("fault_zone"),
               "confidence": (verdict or {}).get("confidence"),
               "evidence": (verdict or {}).get("evidence"),
               "skill": state.get("skill"),
               "skill_dropped": state.get("skill_dropped"),
               "rounds": state.get("rounds"),
               "checks": len(state.get("trace") or []),
               "dry_run": config.DRY_RUN})

    services.open_incidents[app] = {"status": state.get("status"),
                                    "fingerprint": state.get("fingerprint"),
                                    "thread": thread,
                                    "closed_by_human": False,
                                    "at": time.time()}

    return {"slack_thread_ts": thread}


# ================================================================== ACT

def act_node(state, services):
    """Run the remediation. The only node that changes anything.

    Snapshot BEFORE acting. Without a baseline there is nothing to compare
    afterwards, and "it says RUNNING" is not evidence that data is moving.
    """
    app = state["app"]
    thread = state.get("slack_thread_ts")

    services.notifier.set_state(thread, slack_io.ACTING)
    before = act.snapshot(services.striim, app,
                          position=position_of(state.get("evidence") or {}))

    # `did` accumulates what ACTUALLY happened, in order. It is what the
    # final message reports, so it must never contain something the agent
    # merely intended.
    did = []

    repaired, failure, repair_result = _run_repair(state, services, did)
    if failure is not None:
        # The repair itself failed. Nothing was changed, so there is
        # nothing to resume into.
        return {**failure, "did": did, "before_snapshot": before}

    resumed, refusal = _run_resume(state, services, did, repaired=repaired)

    if refusal is not None:
        # A refused RESUME after a SUCCESSFUL repair used to report only the
        # refusal - which meant a write to the customer's database had
        # happened, was never verified, and the thread said "refused". The
        # repair is real and has to be reported and verified whatever the
        # resume did.
        if repaired:
            services.notifier.reply(
                thread, f"⚠ The repair ran, but the resume was refused: "
                        f"{refusal}. Verifying the repair anyway.",
                kind="failure")
            return {"action_result": {"repaired": True, "refused": refusal,
                                      **(repair_result or {})},
                    "did": did, "before_snapshot": before}

        services.notifier.reply(thread, f"⛔ refused: {refusal}",
                                kind="failure")
        return {"action_result": {"refused": refusal}, "did": did,
                "tier": 3, "reason": refusal, "before_snapshot": before}

    return {"action_result": {**resumed, **(repair_result or {})}, "did": did,
            "action_taken": resumed.get("statement") or _join_did(did),
            "before_snapshot": before}


def _run_repair(state, services, did):
    """Whatever fixes the cause, before anything is resumed.

    Returns (repaired, failure_state, outcome). `failure_state` is not None
    only when the repair itself failed, in which case there is nothing to
    resume into and the caller stops. `outcome` is whatever the repair
    reported - it carries the backfill result through to the incident.
    """
    thread = state.get("slack_thread_ts")

    # A skill that reached tier 1 runs its own execute(). Every gate below
    # it - the namespace lock, the verb allowlist, the budget, the repeat
    # check - still applies, because the skill calls the same action layer.
    skill = skill_layer.SKILLS.get(state.get("skill") or "")
    skill_plan = state.get("skill_plan")
    if skill and skill_plan and skill.name != "wait_and_resume":
        try:
            outcome = skill.execute(state, _as_plan(skill_plan), services)
        except AwaitingApproval as waiting:
            # Not a failure. The repair is prepared, announced and held;
            # nothing is resumed until somebody answers, because resuming
            # into an unrepaired target just reproduces the fault.
            return False, {"action_result": {"awaiting_approval": True},
                           "awaiting_approval": waiting.pending["summary"],
                           "approval": _approval_facts(waiting.pending),
                           "tier": 2,
                           "reason": f"waiting for someone to approve: "
                                     f"{waiting.pending['summary']}"}, None
        except Exception as error:
            services.notifier.reply(
                thread, f"⛔ {skill.name} did not run: {error}",
                kind="failure")
            return False, {"action_result": {"error": str(error)},
                           "tier": 3,
                           "reason": f"{skill.name} failed: {error}"}, None

        # A skill that did several things reports them itself, in order, so
        # the incident reads as one repair rather than one headline. A DDL
        # fix followed by a backfill is two statements and one repair.
        did.extend(outcome.get("did") or [skill_plan["summary"]])
        services.notifier.reply(thread, f"Ran {skill.name} — "
                                        f"{skill_plan['summary']}.",
                                kind="action")
        return True, None, outcome

    plan = state.get("repair_plan")
    if plan and plan.get("will_execute"):
        try:
            result = target_repair.execute(plan)
        except Exception as error:
            services.notifier.reply(
                thread, f"⛔ the target repair did not run: {error}",
                kind="failure")
            return False, {"action_result": {"error": str(error)},
                           "tier": 3,
                           "reason": f"target repair failed: {error}"}, None

        rows = result.get("rows_affected")
        did.append(f"changed {rows:,} rows in the target" if rows is not None
                   else "ran the corrective statement in the target")
        services.notifier.reply(
            thread, f"Ran the repair — "
                    f"{f'{rows:,} rows changed' if rows is not None else 'done'}"
                    f". Resuming the pipeline.", kind="action")
        return True, None, result

    return False, None, None


def _run_resume(state, services, did, repaired=False):
    """Start the pipeline again. Returns (result, refusal_sentence)."""
    app = state["app"]
    thread = state.get("slack_thread_ts")

    # A skill that has already restarted the app must not be resumed on top
    # of itself - the property-change cycle ends in START.
    if (state.get("skill_plan") or {}).get("already_restarted"):
        return {"statement": None, "skipped": True}, None

    # THE LAST GATE, and the one that was missing. _run_repair returns
    # "nothing to repair" for every path that has no skill and no executable
    # statement, and ACT fell straight through to here and restarted anyway.
    # Observed live on sarah1_CDC: the agent named region as the blocking
    # column, restarted into it, and reported that the restart did not hold.
    #
    # If nothing was repaired in this pass, the cause must be gone before
    # anything is resumed - measured now, from the databases, not inferred
    # from the diagnosis. A repair that DID just run is exempt: it changed
    # the very thing the evidence describes, so the evidence is stale.
    if not repaired:
        still_broken = cause_still_present(state.get("evidence") or {})
        if still_broken:
            return None, (f"not restarting — {still_broken}")

    try:
        result = act.run_command(services.striim, app, "RESUME",
                                 fingerprint=state.get("fingerprint"),
                                 ledger=services.ledger)
    except act.ActionRefused as error:
        # The last gate. If DECIDE ever gets it wrong, this is what stops it.
        return None, str(error)

    if result.get("dry_run"):
        # Not a state change - nothing changed, which is the point. It goes
        # on the PARENT instead, as the statement a person can run, because
        # otherwise a dry-run incident reads as an agent that saw the fault
        # and then did nothing without saying why.
        state["dry_run_statement"] = result["statement"]
        services.log(f"    [DRY RUN] would have sent {result['statement']}")
    else:
        did.append("resumed the pipeline")
        services.notifier.reply(thread, f"Sent `{result['statement']}`.",
                                kind="action")

    return result, None


def _join_did(did):
    return ", then ".join(did) if did else None


# =============================================================== VERIFY

def close_node(state, services):
    """Rewrite the parent into its final form, and set the final emoji.

    Every terminal path arrives here, so there is exactly ONE place that
    decides what an incident finally says and exactly one place that
    updates the open-incident book. Before this node those decisions were
    spread across TRIAGE, ACT and VERIFY, each handling them slightly
    differently - which is how a message could end up claiming "fixing it"
    long after the agent had stopped.
    """
    app = state["app"]
    thread = state.get("slack_thread_ts")
    if not thread:
        return {}

    record = build_record({**state, "closing": True})
    shape = record["shape"]

    services.notifier.rewrite(
        thread, slack_io.compose_incident(app, state.get("verdict"), record))
    services.notifier.set_state(thread, slack_io.SHAPES[shape][0])

    if shape == "fixed":
        services.open_incidents.pop(app, None)

    return {"final_shape": shape}


def verify_node(state, services):
    """Did it actually work?

    Three outcomes, not two. "Healthy but nothing has moved" is a real and
    common answer - a file source that has already read everything sits
    perfectly healthy with nothing left to prove - and an agent that cannot
    say "I cannot confirm this" ends up saying something false instead.
    """
    app = state["app"]
    thread = state.get("slack_thread_ts")
    result = state.get("action_result") or {}

    # A dry run or a failed repair changed nothing, so there is nothing to
    # verify. A REFUSED RESUME after a successful repair is different: the
    # write happened and must still be checked.
    if result.get("dry_run") or result.get("error"):
        return {"verification": None}
    if result.get("refused") and not result.get("repaired"):
        return {"verification": None}

    # A repair HELD for approval has not run. Nothing was restarted and
    # nothing was written, so there is nothing to verify - and verifying
    # anyway is not merely wasteful, it is a lie: this waited 210 seconds,
    # found the pipeline exactly as it left it, and reported "the restart
    # did NOT work" about a restart that never happened. Observed live on
    # sarah1_CDC: approval requested 23:13:59, "it did not hold" 23:17:36.
    if result.get("awaiting_approval"):
        return {"verification": None}

    outcome = act.verify(services.striim, app,
                         state["before_snapshot"],
                         wait=services.verify_wait,
                         verbose=services.verbose,
                         position_now=lambda: _position_now(state, services))

    services.notifier.reply(thread, slack_io.compose_verification(app, outcome),
                            kind="outcome")

    # How many rows are actually in the target now, as opposed to how many
    # Striim counted moving. Our own finding: the two disagree.
    written = _rows_written(state)

    return {"verification": outcome, "rows_written": written}


def _position_now(state, services):
    """Re-read where the reader has got to, after the wait.

    Freshly, not from the evidence gathered before acting - the whole
    question is whether it has moved since.
    """
    fresh = {}
    slot = _configured_slot(state.get("evidence") or {})
    if config.SOURCE_DB_URL or db.endpoint_for("source"):
        fresh["replication_slot"] = db.db_replication_slot_status(slot)

    component = ((state.get("evidence", {}).get("flow") or {})
                 .get("source") or {}).get("component")
    if component:
        services.striim.invalidate()
        fresh["source_config"] = st.describe_component(services.striim,
                                                       component)
    return position_of(fresh)


def _rows_written(state):
    """The target's real row count delta, or None if it cannot be known."""
    before = (state.get("before_snapshot") or {}).get("target_rows")
    table = (state.get("evidence", {}).get("target_table") or {}).get("table")
    if before is None or not table:
        return None

    after = db.db_row_count("target", table).get("rows")
    if after is None:
        return None
    return max(0, after - before)
