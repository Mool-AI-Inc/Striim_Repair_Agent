"""
What travels through the graph
==============================

One incident, one state object. It starts as just an app name and picks up
evidence as it moves - so at any point you can print it and see exactly
what the agent knew and when it knew it. That is most of why the graph is
worth having.

Nothing here is clever. LangGraph merges whatever a node returns into this
dict, so a node's return value is also the record of what that node did.
"""

import hashlib
from typing import Any, Optional, TypedDict


class IncidentState(TypedDict, total=False):
    # --- identity -------------------------------------------------------
    app: str
    status: str
    fingerprint: str            # dedupe key for this specific fault

    # --- what we found --------------------------------------------------
    evidence: dict              # accumulated tool results
    verdict: Optional[dict]     # the model's structured opinion
    verdict_problem: Optional[str]   # why there is no verdict, if there isn't

    # --- what we decided ------------------------------------------------
    tier: Optional[int]         # 1 act, 2 ask, 3 escalate, None healthy
    reason: Optional[str]       # why that tier, in a sentence
    fault_zone: Optional[str]   # pipeline | target | source | unknown

    # --- which repair, if any -------------------------------------------
    skill: Optional[str]            # the skill that passed its preconditions
    skill_plan: Optional[dict]      # exactly what it would do - always built
    skill_dropped: Optional[str]    # why a suggested skill did NOT apply,
                                    # which is worth saying out loud
    fingerprint_seen: int           # how often this exact fault has recurred
    awaiting_approval: Optional[str]   # the statement somebody must approve
    approval: Optional[dict]           # its counts, backup and reversal, so
                                       # the PARENT can carry the whole ask
    reread_confirmed: bool          # a re-read actually happened
    partial_batch_id: Optional[str]

    # --- what we did ----------------------------------------------------
    action_taken: Optional[str]
    action_result: Optional[dict]
    did: list                       # what ACTUALLY happened, in order - the
                                    # "I did:" line is read off this and
                                    # never off the model
    rows_written: Optional[int]     # the target's real row delta
    final_shape: Optional[str]      # fixed | needs_command | blocked |
                                    # failed | unknown
    before_snapshot: Optional[dict]  # taken BEFORE acting, or there is
                                     # nothing to compare against after
    verification: Optional[dict]
    repair_plan: Optional[dict]     # proposed target DML, if any

    # --- where it was said ----------------------------------------------
    slack_thread_ts: Optional[str]

    # --- housekeeping ---------------------------------------------------
    silent: bool                # true when nothing should reach Slack
    trace: list                 # every tool call made, in order
    rounds: int                 # how many model turns the diagnosis took
    started_at: float
    notes: list                 # anything worth printing at the end


def new_incident(app: str) -> IncidentState:
    return {
        "app": app,
        "evidence": {},
        "trace": [],
        "notes": [],
        "silent": True,
        "tier": None,
        "verdict": None,
    }


def fingerprint(app: str, verdict: Optional[dict], status: Optional[str] = None) -> str:
    """A stable key for THIS fault on THIS app.

    Built from the diagnosed cause rather than the error text, because the
    same cause often produces slightly different text each time and we want
    those to collide. Truncated and hashed so it stays readable in a log
    and stable as a dict key.

    Falls back to the status when there is no verdict - a fault we could not
    diagnose still needs to be recognisable the second time it appears.
    """
    cause = None
    if verdict:
        cause = verdict.get("likely_cause") or verdict.get("headline")
    seed = f"{app}|{str(cause or status or 'undiagnosed')[:120]}".lower()
    digest = hashlib.sha1(seed.encode()).hexdigest()[:10]
    return f"{app}#{digest}"


def short_name(app: str) -> str:
    """Trial5, not sarah_moolai.Trial5. For humans reading a phone."""
    return app.rsplit(".", 1)[-1]


def snapshot_for_log(state: IncidentState) -> dict[str, Any]:
    """The state, minus the bulky bits, for printing at the end of a run."""
    verdict = state.get("verdict") or {}
    return {
        "app": state.get("app"),
        "status": state.get("status"),
        "tier": state.get("tier"),
        "reason": state.get("reason"),
        "zone": state.get("fault_zone"),
        "headline": verdict.get("headline"),
        "confidence": verdict.get("confidence"),
        "action": state.get("action_taken"),
        "verified": (state.get("verification") or {}).get("recovered"),
        "tools_called": len(state.get("trace") or []),
    }
