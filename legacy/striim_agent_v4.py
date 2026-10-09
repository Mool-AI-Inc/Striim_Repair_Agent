"""
Striim SRE agent - version 4
============================

The difference from v3: nobody decides in advance what evidence matters.

v3:  code gathers 6 fixed fields  ->  model explains them
v4:  model is given tools         ->  it decides what to look at, in what
                                      order, based on what it has found

That is the whole change, and it is why v4 can handle failure modes
neither of us thought about. A new kind of incident does not need new
code - it needs the model to ask a different question.

What has NOT changed: the model still only RECOMMENDS. Every guardrail
lives in Python, where it cannot be argued with.

Run:
    set STRIIM_USER / STRIIM_PASS / ANTHROPIC_API_KEY
    python striim_agent_v4.py                      watch mode
    python striim_agent_v4.py sarah_moolai.Trial3  diagnose once, now
"""

import json
import os
import time
from datetime import datetime

import anthropic
import requests

import striim_tools as st
import striim_actions as act

MODEL = "claude-sonnet-5"
MAX_ROUNDS = 8              # stop the model looping forever
POLL_SECONDS = 60
MY_NAMESPACE = st.MY_NAMESPACE

MAX_RESTARTS_PER_HOUR = 2
DRY_RUN = True

# Slack incoming webhook. Unset means print to the terminal instead - the
# agent must never depend on delivery working.
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")


# ========================================================== TOOL SCHEMAS

# What the model is told it can do. Descriptions matter more than you
# would think - this is the only thing telling it WHEN to reach for each
# tool, so they are written as advice, not as documentation.
TOOL_SCHEMAS = [
    {
        "name": "get_status",
        "description": "Current state of one pipeline: is it alive, is it "
                       "faulted, how hard is it working. Start here.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string",
                                   "description": "full name, e.g. ns.AppName"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_errors",
        "description": "The actual error messages for a pipeline. This is "
                       "usually where the real answer is. Always check this "
                       "before concluding you cannot determine a cause. If it "
                       "comes back empty, the text may not be published yet.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_flow_stats",
        "description": "Source and target throughput and timestamps, plus a "
                       "profile of the connector type. Use this to work out "
                       "WHERE data stopped: reads moving but writes flat means "
                       "the target; both flat means the source or the app. The "
                       "connector profile tells you whether silence is normal "
                       "for this kind of source and what restarting risks.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_state_history",
        "description": "Recent state transitions. Use this to answer 'did a "
                       "human do this deliberately?' - a clean shutdown passes "
                       "through STOPPING first. Note looks_deliberate can be "
                       "null, which means NO DATA, not 'no'.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_checkpoints",
        "description": "Whether the recovery bookmark exists and has ever been "
                       "written. Check this before recommending a restart on "
                       "anything where position matters.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_rejected_events",
        "description": "The exception store - whether individual events were "
                       "REFUSED by the target. Check this whenever bad data is "
                       "a possible cause, and whenever a pipeline looks healthy "
                       "but you suspect data is going missing. Rows landing "
                       "here while the pipeline reports RUNNING means data is "
                       "being silently dropped with no alert.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "describe_component",
        "description": "What a component is CONFIGURED to do - for a file "
                       "source, which directory and filename it expects, and "
                       "how far through it had read. Use this whenever a "
                       "source or target has failed and you want to name the "
                       "actual missing thing instead of saying 'check the "
                       "source system'. Component names come from "
                       "get_flow_stats.",
        "input_schema": {
            "type": "object",
            "properties": {"component": {
                "type": "string",
                "description": "full component name, e.g. ns.App_FileReaderSource"}},
            "required": ["component"],
        },
    },
    {
        "name": "list_apps",
        "description": "Every pipeline and its status. Use when you need "
                       "context about the cluster, or to check an exact name.",
        "input_schema": {
            "type": "object",
            "properties": {"namespace": {"type": "string"}},
        },
    },
]


SYSTEM = """You are an on-call SRE for Striim data pipelines, woken at 3am by
an automated watcher. You have tools that read Striim's control plane.

Investigate before concluding. Do not guess from the status word alone -
call the tools you need, in whatever order makes sense, until you either
understand the failure or can say precisely what is missing.

How to think about it:
  HALT       broke due to something OUTSIDE Striim (target down, credentials,
             network, a missing file)
  TERMINATED broke due to something INSIDE (bad data, bad configuration)
  STOPPED / COMPLETED / QUIESCED are NOT faults - someone chose them

Three things a restart can never fix:
  1. The underlying cause is still present (a deleted file is still deleted).
  2. The failure is deterministic (the same bad row fails identically).
  3. Restarting a file-based source can re-read the whole file and append
     every row to the target again. No alert fires for this. The pipeline
     looks healthy while the target quietly fills with duplicates. Check the
     connector profile's restart_risk before recommending any restart.

A checkpoint existing does NOT mean it is usable. Call describe_component on
the source and compare its recorded read position against reality. An offset
far larger than the file could plausibly be means the position accumulated
across earlier file replacements and no longer refers to the current file -
Striim cannot seek to it, so it will re-read from the start and duplicate
everything. Treat a suspicious offset as equivalent to having no checkpoint,
and say so plainly.

Distinguish "I checked and it is not there" from "I have no data". A tool
returning null or empty is not evidence of absence. Say so rather than
inferring from it.

WRITING THE ANSWER

This is read at 3am, on a phone, by someone half asleep. Write for them.

  * Every field is ONE short line. Under 15 words. No sub-clauses.
  * Plain English. No field names, no tool names, no jargon in the text.
    Say "the source file went missing", not "get_errors returned WA-102".
  * No hedging phrases. If you are unsure, lower the confidence instead.
  * Never repeat yourself between fields.
  * If the pipeline is healthy, leave why_not and human_action_needed EMPTY.
    Do not warn about a restart nobody proposed.

Reply with ONLY this JSON - no prose, no fences:

{
  "healthy": true|false,
  "headline": "under 8 words - what is wrong, e.g. 'Source file went missing'",
  "what_happened": "one short line, max 15 words",
  "likely_cause": "one short line, or 'unknown'",
  "confidence": "high"|"medium"|"low",
  "evidence": "one short line - what you saw, in plain words",
  "restart_would_help": true|false|"unknown",
  "why_not": "one short line if restarting is risky, else empty",
  "human_action_needed": "one short line - the single next action, else empty",
  "suggested_tier": 1|2|3,
  "missing_information": "one short line, or empty"
}

Tiers: 1 safe to restart automatically, 2 a human should approve first,
3 nothing to try automatically - escalate with the cause written down.
"""


# ============================================================ THE LOOP

def investigate(client, striim, app, verbose=True):
    """Let the model drive. Returns its verdict as a dict, or None.

    This is the whole difference between v3 and v4. We do not choose the
    evidence - we answer questions until the model stops asking.
    """
    messages = [{
        "role": "user",
        "content": f"Pipeline {app} needs investigating. Work out what is "
                   f"wrong and whether it is safe to restart.",
    }]

    trace = []

    for round_number in range(1, MAX_ROUNDS + 1):
        response = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            system=SYSTEM,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})

        # No tool calls means it has reached a conclusion.
        if response.stop_reason != "tool_use":
            text = "".join(
                block.text for block in response.content
                if getattr(block, "type", None) == "text"
            ).strip()

            if text.startswith("```"):
                text = text.split("```")[1].removeprefix("json").strip()

            try:
                verdict = json.loads(text)
                verdict["_trace"] = trace
                verdict["_rounds"] = round_number
                return verdict
            except json.JSONDecodeError:
                if verbose:
                    print(f"    (round {round_number}: not valid JSON)")
                    print("    " + text[:200])
                return None

        # Otherwise: run whatever it asked for, hand back the results.
        results = []
        for block in response.content:
            if getattr(block, "type", None) != "tool_use":
                continue

            tool = st.TOOLS.get(block.name)
            args = dict(block.input)

            if verbose:
                shown = args.get("app") or args.get("namespace") or ""
                print(f"    [{round_number}] {block.name}({shown})")
            trace.append(f"{block.name}({args})")

            try:
                if tool is None:
                    output = {"error": f"no such tool: {block.name}"}
                else:
                    output = tool(striim, **args)
            except Exception as error:
                # A failing tool must not kill the investigation. Tell the
                # model what went wrong and let it try something else.
                output = {"error": f"{type(error).__name__}: {error}"}

            results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(output, default=str)[:6000],
            })

        messages.append({"role": "user", "content": results})

    if verbose:
        print(f"    (gave up after {MAX_ROUNDS} rounds)")
    return None


# ============================================================== DECIDE

def fingerprint(verdict, app):
    return f"{app}|{str(verdict.get('likely_cause'))[:80]}"


def decide(verdict, app, memory):
    """Code has the final say. The model's tier is a SUGGESTION.

    Note the order: guardrails run BEFORE the model's opinion is consulted,
    and the model can only ever talk its way DOWN to a lower tier.
    """
    if verdict is None:
        return 2, "could not reach a conclusion - human needed", None

    if verdict.get("healthy") is True:
        return None, "no fault found", None

    mark = fingerprint(verdict, app)
    if mark in memory["tried"]:
        return 3, "already attempted a fix for this exact cause", None

    recent = [t for t in memory["restarts"] if time.time() - t < 3600]
    if len(recent) >= MAX_RESTARTS_PER_HOUR:
        return 3, f"restart budget spent ({MAX_RESTARTS_PER_HOUR}/hour)", None

    if not app.startswith(MY_NAMESPACE + "."):
        return 3, f"outside {MY_NAMESPACE} - read only, will not act", None

    if verdict.get("restart_would_help") is not True:
        return 3, verdict.get("why_not") or "a restart would not help", None

    if verdict.get("confidence") != "high":
        return 2, f"confidence is {verdict.get('confidence')}", None

    return 1, verdict.get("likely_cause", "transient"), f"RESUME {app};"


# ============================================================== REPORT

TIER_STYLE = {
    1: ("\U0001F7E2", "fixing it"),
    2: ("\U0001F7E1", "needs your OK"),
    3: ("\U0001F534", "needs you"),
}


def compose(app, verdict, tier, reason):
    """Five lines, readable on a phone, in the dark, half asleep.

    Order matters: what, why not to do the obvious thing, what to do.
    Everything else is a footnote.
    """
    icon, headline = TIER_STYLE.get(tier, ("\u26AA", "all clear"))
    name = app.split(".")[-1]

    if verdict is None:
        return f"{icon}  *{name}* - could not work out what is wrong. {reason}"

    lines = [f"{icon}  *{name}* - {verdict.get('headline', 'fault')} ({headline})"]
    lines.append(f"   {verdict.get('what_happened', '')}")

    # Only warn about restarting when a restart is actually on the table.
    # A healthy pipeline does not need advice about a fix nobody proposed -
    # that is noise, and noise is what gets a channel muted.
    if tier is not None and verdict.get("why_not"):
        lines.append(f"   \u26A0 Don't restart: {verdict['why_not']}")

    action = verdict.get("human_action_needed", "")
    if action and action.strip().lower() not in ("none", "no action", "n/a", ""):
        lines.append(f"   \u2192 Do this: {action}")

    lines.append(
        f"   _{verdict.get('confidence', '?')} confidence \u00B7 "
        f"{verdict.get('evidence', '')}_"
    )
    return "\n".join(lines)


def deliver(text):
    """Post to Slack, or print if there is no webhook.

    Delivery failing must never stop the agent. A message that could not be
    sent still gets printed, so nothing is silently lost.
    """
    if not SLACK_WEBHOOK:
        print(text)
        return

    try:
        response = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=15)
        response.raise_for_status()
        print(text)
        print("   (posted to Slack)")
    except Exception as error:
        print(text)
        print(f"   (Slack delivery failed: {error})")


def report(app, verdict, tier, reason):
    """Send the incident. Healthy pipelines are NOT sent anywhere.

    Only faults reach Slack. An agent that posts "all clear" every minute
    trains people to ignore the channel, which costs more than it gives.
    """
    text = compose(app, verdict, tier, reason)

    if verdict:
        text += (f"\n   _{verdict.get('_rounds')} rounds, "
                 f"{len(verdict.get('_trace', []))} checks_")

    print()
    if tier is None:
        print(text)          # healthy - terminal only, never Slack
    else:
        deliver(text)
    print()


# ================================================================= RUN

def act_and_verify(striim, app, verdict, memory):
    """Tier 1 only: take the snapshot, resume, wait, check it really worked.

    The order matters. Snapshot BEFORE acting - without a baseline there is
    nothing to compare against afterwards, and "it says RUNNING" is not
    evidence that data is moving.
    """
    memory["tried"].add(fingerprint(verdict, app))
    memory["restarts"].append(time.time())

    before = act.snapshot(striim, app)
    print(f"   acting: RESUME {app}")

    try:
        result = act.run_command(striim, app, "RESUME")
    except act.ActionRefused as error:
        deliver(f"   \u26D4 refused: {error}")
        return

    if result.get("dry_run"):
        print(f"   [DRY RUN] would have sent: {result['would_have_run']}")
        print("   (set STRIIM_LIVE=1 to act for real)")
        return

    outcome = act.verify(striim, app, before)

    if outcome["recovered"] is True:
        deliver(f"\u2705  *{app.split('.')[-1]}* recovered - {outcome['detail']}")
    elif outcome["recovered"] is False:
        deliver(f"\U0001F534  *{app.split('.')[-1]}* restart did NOT work - "
                f"{outcome['detail']}. Escalating - a human is needed.")
    else:
        # Neither confirmed nor denied. Say so rather than picking one.
        deliver(f"\U0001F7E1  *{app.split('.')[-1]}* restarted, but "
                f"{outcome['detail']}. Worth a look.")


def diagnose_once(app):
    client = anthropic.Anthropic()
    striim = st.Striim()
    memory = {"tried": set(), "restarts": []}

    print(f"\nInvestigating {app}...")
    verdict = investigate(client, striim, app)
    tier, reason, command = decide(verdict, app, memory)
    report(app, verdict, tier, reason)

    if tier == 1:
        act_and_verify(striim, app, verdict, memory)


def watch(app):
    client = anthropic.Anthropic()
    striim = st.Striim()
    memory = {"tried": set(), "restarts": []}
    previous = None

    print(f"Watching {app}  [{'DRY RUN' if DRY_RUN else 'LIVE'}]\n")

    while True:
        status = st.get_status(striim, app)["status"]
        clock = datetime.now().strftime("%H:%M:%S")

        if status == previous:
            print(f"[{clock}] {status}")
            time.sleep(POLL_SECONDS)
            continue

        previous = status
        print(f"[{clock}] status changed to {status}")

        if status not in st.FAULT_STATES:
            time.sleep(POLL_SECONDS)
            continue

        verdict = investigate(client, striim, app)
        tier, reason, command = decide(verdict, app, memory)
        report(app, verdict, tier, reason)

        if tier == 1:
            act_and_verify(striim, app, verdict, memory)
            previous = None      # re-read state after acting

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    import sys

    target = os.environ.get("STRIIM_APP", f"{MY_NAMESPACE}.Trial3")

    if len(sys.argv) > 1:
        diagnose_once(sys.argv[1])
    else:
        try:
            watch(target)
        except KeyboardInterrupt:
            print("\nStopped.")