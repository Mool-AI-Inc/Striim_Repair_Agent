"""
What the model is told, and what it is allowed to say back
=========================================================

Three things live here: the tool descriptions, the system prompt, and the
parser that turns the model's answer into a dict the rest of the code can
trust.

The tool descriptions matter more than they look. They are the only thing
telling the model WHEN to reach for each tool, so they are written as
advice to a colleague, not as documentation.

The parser matters for a different reason: a malformed verdict must not
become a confident action. parse_verdict returns None and a sentence
explaining what was wrong, and DECIDE turns that into "a human should look
at this" - never into a guess.
"""

import json
import re

# ========================================================== TOOL SCHEMAS

TOOL_SCHEMAS = [
    {
        "name": "get_status",
        "description": "Current state of one pipeline: is it alive, is it "
                       "faulted, how hard is it working, and what that state "
                       "actually MEANS. Start here.",
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
                       "usually where the real answer is. Always check it "
                       "before concluding you cannot determine a cause. If it "
                       "comes back empty, read the note - an empty result "
                       "means something different for a fault two minutes old "
                       "and one two months old.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_flow_stats",
        "description": "Source and target throughput and timestamps, plus a "
                       "behaviour profile for each connector. Use it to work "
                       "out WHERE data stopped: reads moving but writes flat "
                       "means the target; both flat means the source or the "
                       "app itself. The profile tells you whether silence is "
                       "even a symptom for this kind of connector, and what "
                       "restarting it risks. It also gives you the component "
                       "names you need for describe_component.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_state_history",
        "description": "Recent state transitions. Use it to answer 'did a "
                       "human do this deliberately?' - a clean shutdown "
                       "passes through STOPPING first. looks_deliberate can "
                       "be null, which means NO DATA, not 'no'.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_checkpoints",
        "description": "Whether a resumable position exists at all. Check it "
                       "before recommending any restart where position "
                       "matters. A recorded checkpoint is necessary but not "
                       "sufficient - see describe_component.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_rejected_events",
        "description": "The exception store - whether individual events were "
                       "REFUSED by the target. Check it whenever bad data is "
                       "a possible cause, and whenever a pipeline looks "
                       "healthy but you suspect data is going missing. Rows "
                       "landing here while the pipeline reports RUNNING means "
                       "data is being dropped silently with no alert.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "get_rejected_rows",
        "description": "Attempt to read the rows the target refused. ON THIS "
                       "DEPLOYMENT IT WILL NOT RETURN THEM - a console "
                       "SELECT comes back as a query handle rather than "
                       "data, and the retrieval endpoint was probed for and "
                       "not found. get_rejected_events already gives you the "
                       "count, the age and the exact statement, so prefer "
                       "that and put the statement in the incident. Call "
                       "this only if you specifically want to check whether "
                       "the cluster has started answering differently.",
        "input_schema": {
            "type": "object",
            "properties": {
                "app": {"type": "string"},
                "limit": {"type": "integer",
                          "description": "how many rows, default 5, max 20"},
            },
            "required": ["app"],
        },
    },
    {
        "name": "describe_pipeline",
        "description": "Everything about one pipeline at once - status, flow, "
                       "errors, history, checkpoints, rejections. A cheap "
                       "first call when you know nothing yet.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "describe_component",
        "description": "What a component is CONFIGURED to do, and where it "
                       "got to. For a file source: which directory and "
                       "filename it expects, and how far through it had read. "
                       "Use it whenever a source or target has failed and you "
                       "want to name the actual missing thing instead of "
                       "saying 'check the source system'. Also use it to "
                       "sanity-check a recorded read position before trusting "
                       "a checkpoint. Component names come from get_flow_stats.",
        "input_schema": {
            "type": "object",
            "properties": {"component": {
                "type": "string",
                "description": "full component name, e.g. ns.App_FileReaderSource"}},
            "required": ["component"],
        },
    },
    {
        "name": "mon_app",
        "description": "Live per-component rates, queue depths and states, "
                       "straight from the cluster rather than from the "
                       "two-minute-stale health snapshot. Use it when you "
                       "need to know what is happening RIGHT NOW, or which "
                       "component is the one that is stuck.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "mon_component",
        "description": "The same, for a single component - is this one the "
                       "bottleneck?",
        "input_schema": {
            "type": "object",
            "properties": {"component": {"type": "string"}},
            "required": ["component"],
        },
    },
    {
        "name": "show_checkpoint",
        "description": "Checkpoint HISTORY - is the position actually "
                       "advancing, or just being rewritten at the same "
                       "offset? get_checkpoints cannot answer this: the "
                       "health record carries checkpoint times but never a "
                       "position, and a stalled pipeline checkpoints "
                       "forever without moving.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "preflight",
        "description": "Misconfigurations that were wrong from creation "
                       "rather than broken at runtime - a wildcard table "
                       "mapping that makes a pipeline feed itself, change "
                       "capture with recovery disabled, no exception store. "
                       "None of these ever show up as a fault.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "console_read",
        "description": "Run any read-only Striim console command - MON, "
                       "SHOW, LIST, DESCRIBE, STATUS, USAGE, PREVIEW. The "
                       "escape hatch for questions the other tools do not "
                       "cover. Use it rather than concluding a cause cannot "
                       "be determined. SELECT is not available here.",
        "input_schema": {
            "type": "object",
            "properties": {"statement": {
                "type": "string",
                "description": "e.g. 'SHOW sarah_moolai.App CHECKPOINT HISTORY;'"}},
            "required": ["statement"],
        },
    },
    {
        "name": "db_config",
        "description": "Which databases this pipeline actually talks to, "
                       "worked out from its own component configuration - "
                       "host, port, database and user. Use it when a "
                       "database tool says it cannot reach something, to "
                       "find out what it discovered and what is missing. "
                       "Passwords are never returned.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "db_probe",
        "description": "Ask a database directly whether it is up: SELECT 1. "
                       "THE question behind every 'is it safe to restart' - "
                       "Striim can only tell you what it thinks, this tells "
                       "you what is true. Use 'target' or 'source'. Says "
                       "'not configured' plainly when there is no access.",
        "input_schema": {
            "type": "object",
            "properties": {"which": {"type": "string",
                                     "enum": ["target", "source"]}},
            "required": ["which"],
        },
    },
    {
        "name": "db_table_exists",
        "description": "Has a table been renamed or dropped? Use whenever a "
                       "writer reports a missing object, or before saying a "
                       "restart is safe. Naming the missing table is the "
                       "difference between an incident someone can act on "
                       "and one they cannot.",
        "input_schema": {
            "type": "object",
            "properties": {"which": {"type": "string",
                                     "enum": ["target", "source"]},
                           "table": {"type": "string",
                                     "description": "schema.table"}},
            "required": ["which", "table"],
        },
    },
    {
        "name": "db_row_count",
        "description": "How many rows are actually in a table. Striim's "
                       "counters measure work performed, not data present - "
                       "this is the only way to know what is really there.",
        "input_schema": {
            "type": "object",
            "properties": {"which": {"type": "string",
                                     "enum": ["target", "source"]},
                           "table": {"type": "string"}},
            "required": ["which", "table"],
        },
    },
    {
        "name": "db_describe_table",
        "description": "Columns, types, primary key and identity columns. "
                       "Use it to work out WHY a row was refused - a missing "
                       "column, a type mismatch, or a GENERATED ALWAYS "
                       "identity refusing an explicit value. Also tells you "
                       "whether the table can be deduplicated at all.",
        "input_schema": {
            "type": "object",
            "properties": {"which": {"type": "string",
                                     "enum": ["target", "source"]},
                           "table": {"type": "string"}},
            "required": ["which", "table"],
        },
    },
    {
        "name": "db_replication_slot_status",
        "description": "Source only. For a Postgres CDC pipeline the real "
                       "resume position lives in the replication slot, not "
                       "only in Striim's checkpoint, and the two can "
                       "disagree. A slot that is inactive while the app "
                       "reports RUNNING means it is reading nothing.",
        "input_schema": {
            "type": "object",
            "properties": {"slot": {"type": "string",
                                    "description": "optional slot name"}},
        },
    },
    {
        "name": "db_compare_schema",
        "description": "Column-by-column diff between the source and target "
                       "tables. A TABLE EXISTING IS NOT THE SAME AS ITS "
                       "SCHEMA MATCHING - a dropped column leaves the table "
                       "in place while every write fails. Use this whenever "
                       "writes are failing and db_table_exists says the "
                       "table is there. It also gives you the exact ALTER "
                       "statement, with the type taken from the source.",
        "input_schema": {
            "type": "object",
            "properties": {"source_table": {"type": "string"},
                           "target_table": {"type": "string"}},
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "ddl_metrics",
        "description": "How many schema changes from the source were "
                       "captured and NOT applied to the target, when the "
                       "last one was, and what it said. With schema "
                       "evolution set to ignore, this counter ticks the "
                       "moment somebody alters the source - hours before "
                       "anything halts. A non-zero count means the target "
                       "is behind and a write will eventually need what it "
                       "is missing.",
        "input_schema": {
            "type": "object",
            "properties": {"app": {"type": "string"}},
            "required": ["app"],
        },
    },
    {
        "name": "db_compare_constraints",
        "description": "Constraints the TARGET enforces that the SOURCE does "
                       "not - a CHECK, a UNIQUE, a foreign key. Rows the "
                       "source considers perfectly valid get refused, and "
                       "the pipeline keeps RUNNING while they go to the "
                       "exception store, so no status check will ever show "
                       "it. Use whenever rows are being rejected.",
        "input_schema": {
            "type": "object",
            "properties": {"source_table": {"type": "string"},
                           "target_table": {"type": "string"}},
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "db_check_privileges",
        "description": "Can the connected user actually write to this table? "
                       "THE CHECK THAT CLOSES THE ELIMINATION TRAP: a "
                       "revoked INSERT leaves the host answering, the table "
                       "present, the schema matching and db_probe "
                       "succeeding, while every single write fails. If "
                       "writes are failing and everything else looks fine, "
                       "this is what you have not checked - and the answer "
                       "is never 'so it must be the source'.",
        "input_schema": {
            "type": "object",
            "properties": {"which": {"type": "string",
                                     "enum": ["target", "source"]},
                           "table": {"type": "string"}},
            "required": ["which", "table"],
        },
    },
    {
        "name": "db_backfill_preview",
        "description": "Read-only. What a backfill would do, and whether it "
                       "can run at all. Give a COLUMN to ask about a "
                       "restored column's missing values; omit it to ask "
                       "about whole missing ROWS after a table has been "
                       "recreated. Both matter because change capture "
                       "carries changes FORWARD only - a column or table "
                       "put back today stays empty of everything that "
                       "happened before it was put back.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_table": {"type": "string"},
                "target_table": {"type": "string"},
                "column": {"type": "string",
                           "description": "omit for a whole-row backfill"},
            },
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "reconcile",
        "description": "Count both sides and report the drift. A target "
                       "holding MORE rows than the source is what a silent "
                       "re-read looks like. Read-only on both sides.",
        "input_schema": {
            "type": "object",
            "properties": {"source_table": {"type": "string"},
                           "target_table": {"type": "string"}},
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "list_apps",
        "description": "Every pipeline and its status. Use it when you need "
                       "context about the cluster, or to check an exact name.",
        "input_schema": {
            "type": "object",
            "properties": {"namespace": {"type": "string"}},
        },
    },
]


# ========================================================= SYSTEM PROMPT

SYSTEM = """You are an on-call SRE for Striim data pipelines, woken at 3am by
an automated watcher. You have tools that read Striim's control plane.

Investigate before concluding. Do not diagnose from the status word alone -
call the tools you need, in whatever order makes sense, until you either
understand the failure or can say precisely what is missing.

WHAT THE STATES MEAN
  HALT        broke due to something OUTSIDE Striim - target down, network,
              credentials, a missing file
  TERMINATED  broke due to something INSIDE - bad data, bad configuration
  CRASH       the process died
  STOPPED / COMPLETED / QUIESCED are NOT faults. Someone chose them, or the
  work finished. They need no incident and no advice.

WHAT A RESTART CANNOT FIX
  1. A cause that is still present. A deleted file is still deleted.
  2. A deterministic failure. The same bad row fails identically.
  3. A re-read. Restarting a file, batch or spreadsheet source can re-read
     everything and append it to the target again. No alert fires for this.
     The pipeline looks healthy while the target quietly fills with
     duplicates. Check the connector's restart_risk before proposing one.

CHECKPOINTS
A checkpoint existing does NOT mean it is usable. Call describe_component on
the source and look at the recorded read position. An offset far larger than
the file could plausibly be means the position accumulated across earlier
file replacements and no longer refers to the file that is there. Striim
cannot seek to it, so it re-reads from the start and duplicates everything.
Treat a suspicious offset as equivalent to having no checkpoint, and say so.

WHICH ZONE THE FAULT IS IN
Classify every fault before recommending anything. This decides what the
agent is even allowed to do.

  "pipeline"  something in Striim's own control plane - the app stopped, a
              component needs redeploying, a transient failure has cleared.
              The agent can fix these itself.
  "target"    the destination refused, rejected, or half-accepted data.
              Bounded corrective SQL against the target is possible here.
  "source"    the origin is missing, unreachable, or refusing us - a deleted
              file, a dead host, an expired credential, a dropped
              replication slot. The agent NEVER writes to a source. Name the
              exact thing that is wrong and what a person must do.
  "unknown"   you could not tell.

Classify by WHAT WOULD HAVE TO CHANGE to fix it, not by where the error text
came from. A poison row is source data that shows up as a target rejection;
if the fix is deleting rows in the target, that is "target".

"unknown" is treated exactly like "source" - no writes, escalate. Uncertainty
resolves towards the safest zone, never the most convenient one.

EVIDENCE
Distinguish "I checked and it is not there" from "I have no data". A tool
returning null or empty is not evidence of absence; the tools say which one
they mean, in their notes. Read the notes. If you are missing something you
need, say what it is in missing_information rather than working around it.

NEVER PUBLISH A CONDITIONAL ABOUT SOMETHING YOU CAN CHECK. "If the
replication slot is gone…", "if the file is still missing…", "the table may
have been dropped" - each of those is a question, and you are holding the
tool that answers it. Call it. A reader at 3am has less access than you do,
so handing them your uncertainty is handing it to the wrong person.

Say what IS true. "The replication slot striim_slot no longer exists" or
"the slot exists and is inactive" - one of those is the case, and one call
tells you which. If you genuinely cannot establish something, name the
specific unknown and the command that would settle it; do not write a
sentence beginning with "if".

A CHECK THAT SUCCEEDS IS NOT A CLEAN BILL OF HEALTH. This is the single
most repeated mistake in this project, and it wears a different face every
time:

    a table exists          !=  its schema matches
    the host answers        !=  the credentials can write
    the status says RUNNING !=  data is moving
    a checkpoint was written!=  the position advanced
    no rows were rejected   !=  the exception store was readable

Each of those is a narrow question with a narrow answer. Stacking several
narrow yesses does not add up to "this end is fine", and concluding a
SOURCE fault because the target checks passed is the error to watch for.

REJECTIONS WHILE RUNNING ARE A FAULT. If get_rejected_events shows rows
have been refused recently, the pipeline is dropping data on the floor
while every status check passes. Never call that healthy. Say how many rows
were refused, how recently, and that they are NOT in the target.

A STALL IS A FAULT. A pipeline that reports RUNNING while its read position
does not move is broken, and it is the one failure the platform itself
cannot see. The evidence carries a `stall` block - if it says stalled, treat
it as a fault however healthy the status word looks.

A TABLE EXISTING IS NOT A TARGET BEING HEALTHY. db_table_exists answers one
narrow question and it is not "can the pipeline write here". A column
dropped from a target leaves the table exactly where it was while every
write carrying that column fails. Existence and schema are different
questions and the first is no evidence about the second.

So: whenever writes are failing, or the target is a candidate at all, call
db_compare_schema. If it comes back with checked=false the schemas were NOT
compared - that is a gap in your evidence, not a clean result, and you must
say so in missing_information rather than reasoning past it.

DO NOT PLACE A FAULT IN THE SOURCE BY ELIMINATION. "Both ends went quiet
and the target looked fine" is not evidence of a source fault; it is
usually evidence that nothing checked the target properly. A source verdict
needs something positive that points at the source - a missing file, a dead
slot, a rejected credential, an unreachable host. If you have not got one,
the honest zone is "unknown", which is treated as source anyway but does
not claim a cause you cannot show.

EXHAUST THE READ TOOLS BEFORE SAYING YOU CANNOT TELL. If a tool exists and
its output would narrow the cause, call it. "Undetermined" is only an
acceptable answer once there is nothing left to read - and then you must
name the specific command whose output you would need, not wave at a system.

NEVER ASK A HUMAN TO LOOK SOMETHING UP THAT YOU CAN READ. In particular:
never write "inspect the exception store", "check the rejected rows" or
anything like it. Call get_rejected_rows and put the actual row and the
actual constraint in the incident. A person woken at 3am should be told
what is wrong, not sent to go and find out.

NEVER BUILD A TIMELINE OUT OF definition_edited_at. It is when somebody last
edited the pipeline's definition. It has no relationship to when the
pipeline faulted, and an app that was recently redeployed will always show a
recent edit. Saying "terminated shortly after a config change" because those
two numbers are close is inventing a cause. Fault timing comes from
get_state_history and from nowhere else.

REPAIRS
You may suggest ONE repair skill in suggested_skill, or null. Suggesting one
does not run it: the code re-checks the evidence for that skill itself, and
declines if it is not there. Suggest the one that fits; do not argue for it.

{skills}

Suggest a skill whose zone matches your fault_zone. If none fits, use null -
an honest escalation beats a repair that does not apply.

WRITING THE ANSWER
This is read at 3am, on a phone, by someone half asleep. Write for them.

  * Every field is ONE short line. Under 15 words. No sub-clauses.
  * Plain English. No field names, no tool names, no jargon in the text.
    Say "the source file went missing", not "get_errors returned WA-102".
  * No hedging phrases. If you are unsure, lower the confidence instead.
  * Never repeat yourself between fields.
  * If the pipeline is healthy, leave why_not and human_action_needed EMPTY.
    Do not warn about a restart nobody proposed.

Reply with ONLY this JSON - no prose, no code fences:

{
  "healthy": true|false,
  "headline": "under 8 words - e.g. 'Source file went missing'",
  "what_happened": "one short line, max 15 words",
  "likely_cause": "one short line, or 'unknown'",
  "fault_zone": "pipeline"|"target"|"source"|"unknown",
  "confidence": "high"|"medium"|"low",
  "evidence": "one short line - what you saw, in plain words",
  "restart_would_help": true|false|"unknown",
  "why_not": "one short line if restarting is risky, else empty",
  "human_action_needed": "one short line - the single next action, else empty",
  "target_sql": "a single bounded SQL statement, or empty - see below",
  "suggested_skill": "<one skill name from the list above>"|null,
  "suggested_tier": 1|2|3,
  "missing_information": "one short line, or empty"
}

target_sql: only when fault_zone is "target" and you can name a SINGLE
statement that repairs the specific damage you diagnosed - for example
removing the exact duplicate rows you read out of the exception store.
Requirements, all checked in code before anything runs:

  * one statement, DELETE or UPDATE, with a WHERE clause tied to the cause
  * against the table the pipeline actually writes to, fully qualified
  * never TRUNCATE, never DROP, never DDL, never an unqualified DELETE

Leave it empty unless you have read the evidence that names the rows. A
speculative statement is worse than none, and the code will refuse it
anyway. If the repair needs DDL, put the finished statement in
human_action_needed instead - the agent cannot execute DDL and a person
will run it.

Tiers: 1 safe for the agent to restart on its own, 2 a human should approve
first, 3 nothing to try automatically - escalate with the cause written down.
Your tier is a suggestion. The code decides, and it can only lower it."""


def system_prompt():
    """The system prompt with the live skill catalogue filled in.

    Built at call time rather than at import so the skills the model is
    offered can never drift from the skills the code will actually run.
    """
    from .skills import catalogue
    return SYSTEM.replace("{skills}", catalogue())


# =============================================================== PARSING

REQUIRED = ("healthy", "headline", "what_happened", "likely_cause",
            "confidence", "restart_would_help")

# Hedging words. On their own they are harmless English; next to a subject
# below they mean the agent published a question it owns the answer to.
_HEDGE = re.compile(
    r"\b(if|whether|may (?:be|have)|might (?:be|have)|possibly|perhaps|"
    r"presumably|appears to|seems to|could be|in case)\b", re.I)

# Subject -> the tool that settles it. These are facts the agent can look
# up in one call, so a sentence like "if the replication slot is gone" is
# not caution, it is an unanswered question passed to somebody with less
# access than the thing that wrote it.
CHECKABLE = (
    (("replication slot", "slot"), "db_replication_slot_status",
     "whether the replication slot exists and is active"),
    (("table exist", "table is gone", "table was dropped", "target table",
      "still exists"), "db_table_exists", "whether the table exists"),
    (("column", "schema"), "db_compare_schema",
     "which columns differ between source and target"),
    (("permission", "privilege", "grant", "access denied"),
     "db_check_privileges", "which privileges the user actually holds"),
    (("constraint", "check constraint", "unique"), "db_compare_constraints",
     "which constraints the target enforces that the source does not"),
    (("row count", "how many rows", "rows are missing", "drift"),
     "reconcile", "how many rows each side actually holds"),
    (("rejected", "exception store"), "get_rejected_events",
     "how many rows were refused and how recently"),
    (("file", "directory", "path"), "describe_component",
     "which file the reader is configured for and where it got to"),
)


def unresolved_conditionals(verdict, evidence=None):
    """Conditionals about facts a tool could have settled. One per subject.

    Observed live, twice in one run: "If the replication slot is gone…"
    and "If the file is still missing…" - published to a channel by
    something holding `db_replication_slot_status`, `db_table_exists` and
    read access to the source database, none of which it had called.

    A conditional is fine about something genuinely outside reach. About
    something one call away it is worse than useless: it hands the reader
    a question the sender was better placed to answer.
    """
    verdict = verdict or {}
    evidence = evidence or {}
    found = []

    for field in ("why_not", "human_action_needed", "likely_cause",
                  "what_happened", "missing_information"):
        text = str(verdict.get(field) or "")
        if not text or not _HEDGE.search(text):
            continue

        lowered = text.lower()
        for subjects, tool, question in CHECKABLE:
            if not any(subject in lowered for subject in subjects):
                continue
            if any(f["tool"] == tool for f in found):
                continue
            found.append({"field": field, "tool": tool, "question": question,
                          "text": text.strip()})
    return found

VALID_ZONES = {"pipeline", "target", "source", "unknown"}
VALID_CONFIDENCE = {"high", "medium", "low"}


def parse_verdict(text):
    """Turn the model's reply into a verdict dict, or explain why not.

    Returns (verdict, problem). Exactly one of them is None.

    A malformed verdict is a normal event, not an exception. It must never
    become a confident action, and it must never be silently repaired into
    one either - so anything that does not parse cleanly comes back as a
    problem and DECIDE routes it to a human.
    """
    if not text or not text.strip():
        return None, "the model returned nothing"

    body = text.strip()

    # Fenced JSON is common and harmless; unwrap it rather than failing.
    if body.startswith("```"):
        parts = body.split("```")
        if len(parts) >= 2:
            body = parts[1].removeprefix("json").strip()

    # Prose either side of the object is also common. Take the outermost
    # braces rather than giving up - but only after trying the whole string,
    # so a genuinely malformed reply is not salvaged into a wrong one.
    try:
        verdict = json.loads(body)
    except json.JSONDecodeError:
        start, end = body.find("{"), body.rfind("}")
        if start == -1 or end <= start:
            return None, "the model's answer was not JSON"
        try:
            verdict = json.loads(body[start:end + 1])
        except json.JSONDecodeError:
            return None, "the model's answer was not valid JSON"

    if not isinstance(verdict, dict):
        return None, "the model's answer was not an object"

    missing = [key for key in REQUIRED if key not in verdict]
    if missing:
        return None, f"the verdict was missing {', '.join(missing)}"

    if not isinstance(verdict.get("healthy"), bool):
        return None, "the verdict did not say clearly whether it is healthy"

    # Normalise the two fields the guardrails branch on. Anything the code
    # does not recognise becomes the cautious value, never the convenient one.
    zone = str(verdict.get("fault_zone", "unknown")).strip().lower()
    verdict["fault_zone"] = zone if zone in VALID_ZONES else "unknown"

    confidence = str(verdict.get("confidence", "")).strip().lower()
    verdict["confidence"] = (confidence if confidence in VALID_CONFIDENCE
                             else "low")

    # Normalise, but do NOT validate against the registry here - an unknown
    # skill name is not a malformed verdict, it is a suggestion the code
    # will decline, and declining it visibly is more useful than throwing
    # the whole diagnosis away.
    suggested = verdict.get("suggested_skill")
    if isinstance(suggested, str):
        suggested = suggested.strip().lower() or None
        verdict["suggested_skill"] = None if suggested in (
            "null", "none", "n/a", "-") else suggested
    else:
        verdict["suggested_skill"] = None

    return verdict, None
