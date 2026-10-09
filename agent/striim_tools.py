"""
Striim tool layer
=================

The agent's senses. Nothing here changes a pipeline, and the console helper
refuses any verb that could.

Two different safety rules, deliberately:
  READS  - allowed anywhere on the cluster. Diagnosing someone else's
           pipeline is harmless, and it is how we show the agent is not
           file-reader-specific.
  WRITES - live in striim_actions.py and are locked to one namespace.

ONE HONEST EXCEPTION, and it is worth stating rather than hiding. A console
SELECT is not free: Striim compiles it into a transient application in the
GLOBAL namespace. Captured evidence, from a real health record taken while
nothing but a console query was running:

    stateChangeList: Global.adhocquery1f191219d7402c1b1872af9bf95d7f2
                     type APPLICATION, currentStatus CREATED

Those never appear in appHealthMap, so they are short-lived, but they are
real objects created outside the namespace this agent is otherwise locked
to. Consequences, all deliberate:

  * SELECT is NOT in READ_ONLY_VERBS and there is no general SELECT tool.
    The model cannot ask for arbitrary SQL against the cluster.
  * The one place a SELECT is issued - get_rejected_rows - builds the
    statement itself from a store name it has verified, caps the row limit,
    and is called once per incident.

THE DESIGN RULE FOR EVERY TOOL HERE: it must be able to say "I don't know"
as something different from "no". An empty result is not evidence of
absence. Three of the five bugs found while prototyping were tools
reporting missing data as a negative finding, which walked the model into
confident wrong conclusions. Where a tool cannot tell, it says so in the
note, in words, because the note is what the model actually reads.

THE DESIGN RULE FOR EVERY TOOL HERE: it must be able to say "I don't know"
as something different from "no". An empty result is not evidence of
absence. Three of the five bugs found while prototyping were tools
reporting missing data as a negative finding, which walked the model into
confident wrong conclusions. Where a tool cannot tell, it says so in the
note, in words, because the note is what the model actually reads.

Try any tool on its own:
    python -m agent tools list_apps
    python -m agent tools get_status sarah_moolai.Trial3
    python -m agent tools describe_pipeline sarah_moolai.Trial5
"""

import json
import re
import time

import requests

from . import config
from . import db as _db
from .profiles import profile_for

STRIIM_URL = config.STRIIM_URL
MY_NAMESPACE = config.NAMESPACE

FAULT_STATES = {"HALT", "TERMINATED", "CRASH"}
CALM_STATES = {"RUNNING", "COMPLETED", "STOPPED", "QUIESCED",
               "CREATED", "DEPLOYED"}

# Console verbs this module is willing to send. Genuine control-plane reads
# only - the file stays read-only even if someone hands it a string from
# somewhere else.
#
# SELECT is NOT here, on purpose. It creates a transient Global.adhocquery
# application (see the module docstring), so it is not a read in the sense
# the rest of this list means. The single SELECT the agent issues goes
# through _select(), below, which is not reachable from a model tool call.
READ_ONLY_VERBS = {"DESCRIBE", "LIST", "SHOW", "USE", "STATUS", "MON",
                   "USAGE", "PREVIEW"}

# The most rows the agent will ever pull out of an exception store. This is
# diagnosis, not extraction - a handful of rejected rows tells you the
# constraint that is failing, and ten thousand tells you the same thing
# while filling the model's context with customer data.
MAX_REJECTED_ROWS = 20


class NotReadOnly(Exception):
    """A write verb reached the read-only transport. Never caught silently."""


# ============================================================== TRANSPORT

class Striim:
    """One door to Striim. Everything goes through here.

    Can also be pointed at a captured health record instead of the network
    (`Striim.offline(path)`), which is how the acceptance tests run every
    scenario without breaking a live pipeline first.
    """

    def __init__(self, fixture=None):
        self._cache = None
        self._cached_at = 0
        self._fixture = fixture
        self.is_offline = fixture is not None
        # Offline only: flipped by the action layer so a fixture can
        # describe the world before AND after a restart. Without it every
        # offline verification reads the pre-action snapshot and reports a
        # working fix as a failure - which is exactly the mistake the
        # 210-second wait exists to prevent against the real cluster.
        self.acted = False
        self.token = "offline" if self.is_offline else None
        if not self.is_offline:
            self.login()

    # ---------------------------------------------------------- offline

    @classmethod
    def offline(cls, path):
        """Build a client backed by a captured health record.

        Fixture shape:
            {"health": {...the healthRecords[0] object...},
             "console": {"DESCRIBE ns.X;": <parsed response>}}

        Any value written as {"$ago_ms": 90000} is rewritten at load time to
        "90 seconds before now", so a fixture stays meaningful next week.
        """
        import os
        with open(path) as handle:
            fixture = _resolve_relative_times(json.load(handle))
        fixture.setdefault("name", os.path.basename(path))
        return cls(fixture=fixture)

    # ------------------------------------------------------------ live

    def login(self):
        import os
        response = requests.post(
            f"{STRIIM_URL}/security/authenticate",
            data={"username": os.environ["STRIIM_USER"],
                  "password": os.environ["STRIIM_PASS"]},
            timeout=30,
        )
        response.raise_for_status()
        self.token = response.json()["token"]

    def health(self, max_age_seconds=20):
        """The cluster health record.

        Cached briefly: one diagnosis may call six tools and there is no
        sense fetching the same snapshot six times. /health only refreshes
        every couple of minutes anyway - see FINDINGS.md - so this cache
        costs nothing in freshness.
        """
        if self.is_offline:
            if self.acted and self._fixture.get("health_after_action"):
                return self._fixture["health_after_action"]
            return self._fixture.get("health", {})

        if self._cache and (time.time() - self._cached_at) < max_age_seconds:
            return self._cache

        response = requests.get(f"{STRIIM_URL}/health",
                                params={"token": self.token}, timeout=30)
        if response.status_code in (401, 403):
            self.login()
            response = requests.get(f"{STRIIM_URL}/health",
                                    params={"token": self.token}, timeout=30)
        response.raise_for_status()

        self._cache = response.json()["healthRecords"][0]
        self._cached_at = time.time()
        return self._cache

    def invalidate(self):
        """Drop the cache. Used before a verification read, where a stale
        snapshot would answer a question about the wrong point in time."""
        self._cache = None
        self._cached_at = 0


def _resolve_relative_times(blob):
    """Rewrite {"$ago_ms": N} into a real epoch-milliseconds timestamp."""
    now_ms = time.time() * 1000

    def walk(node):
        if isinstance(node, dict):
            if set(node) == {"$ago_ms"}:
                return round(now_ms - node["$ago_ms"])
            return {key: walk(value) for key, value in node.items()}
        if isinstance(node, list):
            return [walk(item) for item in node]
        return node

    return walk(blob)


# ================================================================= TOOLS

def _ago(timestamp_ms):
    if not timestamp_ms:
        return None
    return round((time.time() * 1000 - timestamp_ms) / 1000)


def _when(timestamp_ms):
    """An absolute wall-clock string, never a relative one.

    Relative ages invite arithmetic. Where a value must not be used to build
    a timeline, giving it as a date stops the subtraction happening at all.
    """
    if not timestamp_ms:
        return None
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp_ms / 1000))


def _has_ever(timestamp_ms):
    """True / False / None - happened, never happened, not reported.

    Striim writes a literal 0 for "this has never occurred". Collapsing that
    into the same None as a missing field would lose a real finding: a CDC
    source with lastEventTime 0 has never read a single change.
    """
    if timestamp_ms is None:
        return None
    return timestamp_ms != 0


def list_apps(striim, namespace=None):
    """Every pipeline visible, with its status. Optionally one namespace."""
    apps = striim.health().get("appHealthMap", {})

    result = {}
    for name, info in apps.items():
        if namespace and not name.startswith(namespace + "."):
            continue
        result[name] = info.get("status")

    counts = {}
    for status in result.values():
        counts[status] = counts.get(status, 0) + 1

    return {"count": len(result), "by_status": counts, "apps": result}


# Zero throughput means different things in different states, and the status
# word alone cannot tell them apart: an app that never started looks exactly
# like one that finished, and like one that has quietly stalled. Spell out
# what each state MEANS rather than leaving the model to infer it.
STATUS_MEANING = {
    "CREATED":    "written but never placed on a machine - has never run",
    "DEPLOYED":   "placed on a machine but NOT STARTED - has never read "
                  "anything. Zero throughput is expected and means nothing.",
    "DEPLOYING":  "being placed on a machine right now - transient",
    "STARTING":   "coming up right now - transient",
    "RUNNING":    "started and live",
    "COMPLETED":  "ran and finished its work normally - not a fault",
    "STOPPING":   "being shut down by someone right now - not a fault",
    "STOPPED":    "switched off deliberately by someone - not a fault",
    "QUIESCING":  "being paused cleanly right now - not a fault",
    "QUIESCED":   "paused cleanly with in-flight data flushed - not a fault",
    "HALT":       "broke because of something OUTSIDE Striim",
    "TERMINATED": "broke because of something INSIDE the pipeline",
    "CRASH":      "the process itself died",
    "NOT RUNNING": "not currently running - check the state history for why",
}


def get_status(striim, app):
    """Is it alive, and what does that state actually mean?"""
    info = striim.health().get("appHealthMap", {}).get(app)
    if info is None:
        return {"app": app, "status": "NOT FOUND",
                "status_means": "no such application in the health record",
                "is_fault": False,
                "note": "check the exact name including the namespace. This "
                        "is 'I cannot see it', which is NOT the same as 'it "
                        "is not there' - a brand new app may not have "
                        "appeared in the snapshot yet."}

    status = info.get("status")

    return {
        "app": app,
        "status": status,
        "status_means": STATUS_MEANING.get(status, "unrecognised state - do "
                                                   "not guess what it means"),
        "has_ever_run": status not in ("CREATED", "DEPLOYED"),
        "is_fault": status in FAULT_STATES,
        "throughput_rate": info.get("rate"),
        "cpu_rate": info.get("cpuRate"),

        # DELIBERATELY NOT a "seconds ago" number, and deliberately renamed.
        #
        # This is when the application DEFINITION was last edited. It has no
        # relationship to when anything happened at runtime. But a number
        # sitting next to a fault is an invitation to subtract, and that
        # invitation was accepted - observed live, from this very field:
        #
        #   "Terminated shortly after config edit. App terminated; definition
        #    was edited about 16 minutes before fault."
        #
        # There was no relationship. The app had just been redeployed. A
        # plausible causal story assembled out of a coincidence is precisely
        # the failure this whole design exists to prevent, so the arithmetic
        # is removed at the source rather than argued with in the prompt.
        "definition_edited_at": _when(info.get("lastModifiedTime")),
        "definition_edited_at_warning": (
            "This is when the pipeline's DEFINITION was last edited. It is "
            "NOT when anything happened at runtime and it is NOT related to "
            "this fault. Do not build a timeline from it, do not describe "
            "the fault as following it, and never cite it as a cause. Fault "
            "timing comes from get_state_history and from nowhere else."),
    }


def fault_age(striim, app):
    """How long ago the app entered its current fault state, or None.

    None means "the transition has aged out of the rolling window", not
    "it just happened". Callers must keep those apart.
    """
    for change in reversed(striim.health().get("stateChangeList", [])):
        if change.get("fqName") == app and change.get("currentStatus") in FAULT_STATES:
            return _ago(change.get("timestamp"))
    return None


def get_errors(striim, app):
    """The actual error messages. Usually the answer lives here."""
    record = striim.health()
    issues = [i for i in record.get("issuesList", [])
              if i.get("fqName") == app]

    if issues:
        return {
            "app": app,
            "error_count": len(issues),
            "errors": [i.get("issue") for i in issues[:5]],
            "note": None,
        }

    # Nothing found - but WHY nothing was found matters, and the honest
    # answer depends on how old the fault is. Telling the model "try again
    # in a minute" about an app that broke eight months ago sends it down a
    # false path. A tool's wording shapes the reasoning as much as its data.
    #
    # Age the fault from the STATE HISTORY, which carries real transition
    # timestamps. The app record's lastModifiedTime is when the definition
    # was last EDITED - using it here would invent a fault age from nothing.
    age = fault_age(striim, app)

    # If the WHOLE list is empty, not just this app's slice, then no
    # application anywhere on the cluster is publishing issue text - which
    # is a fact about the deployment, not about this pipeline. Observed on
    # the real cluster: 228 apps in HALT, issuesList length zero. Saying
    # "no error was recorded for Trial5" there would imply Trial5 is unusual
    # when it is not, and would send someone looking for a cause that no
    # amount of waiting will produce.
    if not record.get("issuesList"):
        return {
            "app": app,
            "error_count": 0,
            "errors": [],
            "fault_age_seconds": age,
            "cluster_publishes_errors": False,
            "note": "no error text - and this cluster is publishing NO issue "
                    "text for any application at all, so this says nothing "
                    "specific about this pipeline. Do not wait for text that "
                    "is not coming, and do not treat its absence as evidence. "
                    "Get the cause from describe_component and the flow "
                    "statistics instead.",
        }

    if age is None:
        note = ("no error text found, and the age of this fault is unknown - "
                "its transition has aged out of the recent-change window. So "
                "this could be a fault whose text is still pending, or an old "
                "one that never recorded any. Do not assume either.")
    elif age < 300:
        note = (f"no error text yet - this fault is only {age}s old and the "
                "text is usually published a minute or two after the status "
                "changes. Worth checking again shortly.")
    else:
        hours = max(1, round(age / 3600))
        note = (f"no error text, and this fault is about {hours} hour(s) old. "
                "The text is not pending - none was ever recorded, or it has "
                "since been cleared. Do not wait for it; reason from the "
                "other evidence, or say plainly that the cause is unavailable.")

    return {
        "app": app,
        "error_count": 0,
        "errors": [],
        "fault_age_seconds": age,
        "note": note,
    }


def _components_for(record, app, section):
    """The source/target components belonging to one app.

    The documented convention is <App>_<Adapter><Role>, so a prefix match on
    the short name is the strict test. Real clusters do not stick to it -
    Trial5's source is `sarah_moolai.Trial5_FileReaderSource` but its target
    is `sarah_moolai.File_Trial5_Target`, with the app name in the MIDDLE.

    So there is a fallback, but it matches whole name-parts rather than a
    bare substring. On a cluster with 576 pipelines a substring test would
    hand Trial5 the components of Trial50.
    """
    namespace, _, short = app.rpartition(".")
    everything = record.get(section, {}) or {}

    strict = {n: i for n, i in everything.items()
              if n.startswith(f"{namespace}.{short}_")}
    if strict:
        return strict

    def parts(name):
        return set(name.replace(".", "_").split("_"))

    return {n: i for n, i in everything.items()
            if n.startswith(namespace + ".") and short in parts(n)}


def _bytes(value):
    """Striim formats byte offsets with thousands separators: "170,969"."""
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError, AttributeError):
        return None


def _lag(raw):
    """End-to-end lag, which is a nested statistics blob rather than a number.

    Observed shape:
      {"data": [{"recentStats": {"latest": 0.08, ...},
                 "type": "Idle", "lee": 0.08, ...}]}
    ...and plain null on an app that has never moved anything.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return {"seconds": raw}

    try:
        entry = (raw.get("data") or [{}])[0]
    except (AttributeError, IndexError, TypeError):
        return {"unparsed": str(raw)[:200]}

    stats = entry.get("recentStats") or {}
    return {"latest": stats.get("latest") if stats else entry.get("lee"),
            "average": stats.get("average"),
            "state": entry.get("type")}


def get_flow_stats(striim, app):
    """Rates and timestamps for the source and target ends, with the
    behaviour profile of each connector attached.

    Reads climbing while writes flatline means the target is the problem.
    Both flat means the source, or the app itself. The profile is what says
    whether "flat" is even a symptom: silence is the normal end state for a
    file reader and a red flag for change capture.
    """
    record = striim.health()

    sources = _components_for(record, app, "sourceHealthMap")
    targets = _components_for(record, app, "targetHealthMap")

    src_name, src = next(iter(sources.items()), (None, {}))
    tgt_name, tgt = next(iter(targets.items()), (None, {}))

    result = {
        "app": app,
        "source": {
            "component": src_name,
            "event_rate": src.get("eventRate"),
            "seconds_since_last_event": _ago(src.get("lastEventTime")),
            # A literal 0 means "never", which is a different fact from a
            # missing field meaning "not reported". For a CDC source, never
            # having read anything is a finding in itself.
            "has_ever_read": _has_ever(src.get("lastEventTime")),
            "profile": profile_for(src_name, role="source"),
        },
        "target": {
            "component": tgt_name,
            "event_rate": tgt.get("eventRate"),
            "seconds_since_last_write": _ago(tgt.get("lastWriteTime")),
            "has_ever_written": _has_ever(tgt.get("lastWriteTime")),
            "end_to_end_lag": _lag(tgt.get("lagEnd2End")),
            "profile": profile_for(tgt_name, role="target"),
        },
    }

    if src_name is None and tgt_name is None:
        result["note"] = ("no source or target components found for this app "
                          "in the health record. That is 'I cannot see them', "
                          "not 'it has none' - the app may never have been "
                          "deployed, or the components may be named "
                          "unconventionally.")
    if len(sources) > 1 or len(targets) > 1:
        result["note"] = (f"this app has {len(sources)} source(s) and "
                          f"{len(targets)} target(s); only the first of each "
                          "is summarised above.")
        result["all_sources"] = list(sources)
        result["all_targets"] = list(targets)

    return result


def get_state_history(striim, app):
    """Recent state transitions.

    The question this answers: did a human ask for this? A pipeline stopped
    on purpose passes through STOPPING first. One that broke does not.
    """
    changes = [c for c in striim.health().get("stateChangeList", [])
               if c.get("fqName") == app]

    transitions = [
        {"from": c.get("previousStatus"),
         "to": c.get("currentStatus"),
         "seconds_ago": _ago(c.get("timestamp"))}
        for c in changes[-6:]
    ]

    # CRITICAL: an empty list means "I have no data", NOT "it was not
    # deliberate". This list is a short rolling window of recent changes
    # across the whole cluster; anything older has aged out of it. Reading
    # silence as "nobody asked for this" is exactly how an agent talks
    # itself into restarting something a human switched off on purpose.
    if not transitions:
        return {
            "app": app,
            "transitions": [],
            "looks_deliberate": None,
            "note": "NO DATA. The state change list is a short rolling window "
                    "and this app's transitions have aged out of it. This "
                    "tells you nothing about whether the change was "
                    "deliberate - do not treat it as evidence either way.",
        }

    latest = changes[-1]
    landed_on = latest.get("currentStatus")
    path = [c.get("currentStatus") for c in changes]

    # The STOPPING test only describes a transition INTO a stopped state.
    # Applying it to a START is a category error: nothing starts itself, so
    # STOPPED -> RUNNING is always requested by someone, and the absence of
    # STOPPING there means nothing at all.
    if landed_on in ("RUNNING", "DEPLOYED", "CREATED", "STARTING", "DEPLOYING"):
        return {
            "app": app,
            "transitions": transitions,
            "looks_deliberate": True,
            "note": f"the most recent transition ended in {landed_on}, which "
                    "is a start. Starts are always requested - by a person or "
                    "by auto-resume - so the STOPPING test does not apply.",
        }

    deliberate = "STOPPING" in path or "QUIESCING" in path

    return {
        "app": app,
        "transitions": transitions,
        "looks_deliberate": deliberate,
        "note": ("passed through STOPPING - a human almost certainly did this, "
                 "so it is not a fault and needs no incident"
                 if deliberate else
                 f"reached {landed_on} with no STOPPING transition - nobody "
                 "asked for it, so it broke"),
    }


def get_checkpoints(striim, app):
    """Is there a position this app could actually resume from?

    Three different answers, and they are not the same:
      recovery off          - every start is a cold start
      recovery on, no data  - the structure exists, nothing was ever written
      recovery on, recorded - there is a position; whether it is USABLE is
                              a separate question, see describe_component
    """
    info = striim.health().get("appHealthMap", {}).get(app, {})
    checkpoints = info.get("checkpointInformation")

    if not checkpoints:
        return {
            "app": app,
            "recovery_enabled": False,
            "has_usable_checkpoint": False,
            "checkpoint_times": [],
            "note": "no checkpoint data at all - recovery is not enabled on "
                    "this application, so it cannot resume from a stored "
                    "position. Every start is a cold start, which for a file "
                    "or batch source means re-reading everything. Recovery "
                    "can only be set when the app is created.",
        }

    # The real shape is a LIST, not the per-component map you might expect:
    #   [{"firstRecordedCheckpoint": {...}},
    #    {"lastTwoRecordedCheckpoints": [{...}, {...}]}]
    # and each entry carries a checkpointTime, a duration and a type - but
    # NO read position. The position lives in describe_component, which is
    # why concluding "safe to restart" from this tool alone cannot work.
    #
    # The structure also exists in full while every value inside it is the
    # string "N/A": recovery switched on, no checkpoint ever written. Half
    # the faulted apps on the cluster look like that. Reading only the outer
    # structure would report "recovery is on, safe to resume" and be wrong
    # in the most expensive direction available.
    times = [entry.get("checkpointTime") for entry in _walk_checkpoints(checkpoints)]
    recorded = [t for t in times if t and t != "N/A"]

    return {
        "app": app,
        "recovery_enabled": True,
        "has_usable_checkpoint": bool(recorded),
        "checkpoint_times": recorded,
        "last_checkpoint_time": recorded[-1] if recorded else None,
        "checkpoints": checkpoints,
        "note": (
            "checkpoints HAVE been written - the most recent was at "
            f"{recorded[-1]} by the server's clock. That is necessary but NOT "
            "sufficient to restart safely: this tool reports only WHEN a "
            "checkpoint happened, never WHERE the reader had got to. Call "
            "describe_component on the source for the read position, and "
            "judge whether that position still refers to the data that is "
            "actually there."
            if recorded else
            "recovery is enabled but every recorded checkpoint is N/A - none "
            "was ever written. There is no position to resume from, so this "
            "behaves exactly like recovery being off."
        ),
    }


def _walk_checkpoints(node):
    """Yield every checkpoint record in the nested list/dict structure."""
    if isinstance(node, dict):
        if "checkpointTime" in node:
            yield node
            return
        for value in node.values():
            yield from _walk_checkpoints(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_checkpoints(item)


def get_rejected_events(striim, app):
    """The exception store - individual events the target refused.

    This is where a poison event actually lives: not "something failed" but
    the specific row that could not be delivered. Rows landing here while
    the pipeline reports RUNNING means data is being dropped silently and
    no alert will ever fire about it.
    """
    record = striim.health()
    stores = record.get("waStoreHealthMap", {}) or {}

    expected = f"{app}_ExceptionStore"
    store = stores.get(expected)

    if store is None:
        return {
            "app": app,
            "exception_store_exists": False,
            "has_ever_rejected": None,
            "note": "no exception store on this application, so rejected "
                    "events are not captured anywhere. If the target is "
                    "refusing rows there is no record of which ones - this "
                    "is 'I cannot tell', not 'nothing was rejected'. The "
                    "store is set with USE EXCEPTIONSTORE at creation.",
        }

    last_write = store.get("lastWriteTime")
    rejected = last_write not in (None, 0)

    return {
        "app": app,
        "exception_store_exists": True,
        "store_name": expected,
        "write_rate": store.get("writeRate"),
        "seconds_since_last_rejection": _ago(last_write) if rejected else None,
        "has_ever_rejected": rejected,
        # The rows themselves are NOT readable over REST. A console SELECT
        # returns a query handle, not data - probed directly against the
        # live cluster, all three retrieval routes tried, none worked.
        # Pointing the model at get_rejected_rows here would send it to
        # spend a round trip creating a transient application and coming
        # back with the same statement this note already carries.
        #
        # So: say that rejections happened, how many and how recently, and
        # hand over the statement. That is everything that can honestly be
        # offered, and it costs nothing to produce.
        "read_rows_with": f"SELECT * FROM {expected} LIMIT 5;",
        "note": (
            f"events HAVE been rejected into the exception store - the target "
            f"refused them"
            + (f", most recently {_ago(last_write)}s ago" if rejected else "")
            + f" at {store.get('writeRate')} rows/second. The rows themselves "
              f"cannot be read through this API: a console SELECT returns a "
              f"query handle rather than data. Report the rejection, the "
              f"count and the age, and give whoever reads the incident this "
              f"statement to run in the Striim console:  "
              f"SELECT * FROM {expected} LIMIT 5;   Do NOT write 'inspect "
              f"the exception store' - name the statement."
            if rejected else
            "the exception store exists and nothing has ever been rejected "
            "into it. No individual events have been refused by the target."
        ),
    }


def get_rejected_rows(striim, app, limit=5):
    """The ACTUAL rejected rows, not merely whether any exist.

    get_rejected_events says the store has received something.  This says
    WHICH rows and WHY, which is the difference between an alert and a
    diagnosis:

        before   "Inspect exception store rows to find rejection cause"
        after    "Row id=9 rejected: duplicate key on target_pkey. 3 more."

    Two things about how this is written.

    The statement is built HERE, from a store name checked against the
    health record first. The model names an application; it never supplies
    SQL. A console SELECT creates a transient Global.adhocquery application
    (see the module docstring), so this is the one place in the read layer
    with a side effect on the cluster, and it is not going to be reachable
    by a model that has talked itself into wanting to run something else.

    And the response shape is UNVERIFIED. This endpoint has never been seen
    returning store rows - only DESCRIBE output - so rather than assume a
    shape and mis-parse it, every outcome is classified and reported for
    what it is. `rows: []` is never allowed to mean "I could not read
    them"; `shape` says which of the two happened.
    """
    limit = max(1, min(int(limit or 5), MAX_REJECTED_ROWS))

    # Check the store exists before asking for its contents, so a typo in
    # the app name comes back as "no such store" rather than as a console
    # error the model has to interpret.
    existing = get_rejected_events(striim, app)
    if not existing.get("exception_store_exists"):
        return {"app": app, "shape": "no_store", "rows": [],
                "statement": None,
                "note": existing.get("note")}

    store = existing["store_name"]
    statement = f"SELECT * FROM {store} LIMIT {limit};"

    result = {"app": app, "store_name": store, "statement": statement,
              "rows": [], "row_count": 0}

    try:
        raw = _send(striim, statement)
    except Exception as error:
        # Mandate: never "go and check X". If the agent could not run it,
        # it hands over the exact statement and says why it could not.
        return {**result, "shape": "error",
                "error": f"{type(error).__name__}: {error}",
                "note": f"could not read the exception store. Run this in "
                        f"the Striim console to see the rejected rows:  "
                        f"{statement}"}

    return _classify_rows(result, raw)


# Observed once, live, from a SELECT against an exception store:
#
#   adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 01f19293-c455-ef51-b6f6-22ca1b2e804a
#
# Four fields: the ad-hoc query application Striim compiled the statement
# into, the literal word QUERY, a small integer, and a UUID. The
# application name matches the Global.adhocquery<hex> entries seen in
# /health's stateChangeList, which is what confirmed these are real
# applications rather than opaque tokens.
_QUERY_HANDLE = re.compile(
    r"^\s*(?P<query_name>adhocquery[0-9a-f]+)\s+QUERY\s+(?P<index>\d+)\s+"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f-]{20,})\s*$", re.I)


def parse_query_handle(text):
    """Pull the query name and UUID out of a console handle, or None.

    Parsed rather than treated as opaque so that the incident can name the
    query, and so that wiring up a results endpoint later is one function
    rather than a re-investigation.
    """
    match = _QUERY_HANDLE.match(str(text or ""))
    if not match:
        return None
    found = match.groupdict()
    found["index"] = int(found["index"])
    found["fq_name"] = f"Global.{found['query_name']}"
    return found


def _classify_rows(result, raw):
    """Work out what came back, and never guess.

    Five outcomes. The parser for Striim's box-drawing tables is
    deliberately NOT written until a response is observed that needs one -
    building it now would mean building against a fixture we wrote
    ourselves, which is the exact mistake FINDINGS sections 11 to 16 record.
    """
    statement = result["statement"]

    try:
        entry = raw[0] if isinstance(raw, list) else raw
        output = entry.get("output")
        status = entry.get("executionStatus")
        code = entry.get("responseCode")
    except (AttributeError, IndexError, KeyError, TypeError):
        return {**result, "shape": "unknown", "raw": str(raw)[:800],
                "note": f"the console answered in a shape this tool does not "
                        f"recognise. Run this in the Striim console to see "
                        f"the rejected rows:  {statement}"}

    if (status and status != "Success") or (code and code != 200):
        return {**result, "shape": "error",
                "error": f"{status} / {code}",
                "raw": str(entry)[:600],
                "note": f"the console refused the query ({status}). Run this "
                        f"in the Striim console to see the rejected rows:  "
                        f"{statement}"}

    # The hoped-for case: a list of row dicts.
    if isinstance(output, list) and output and all(isinstance(r, dict) for r in output):
        rows = output[:MAX_REJECTED_ROWS]
        return {**result, "shape": "rows", "rows": rows, "row_count": len(rows),
                "note": _summarise_rejections(rows)}

    if isinstance(output, list) and not output:
        return {**result, "shape": "rows", "rows": [], "row_count": 0,
                "note": "the query ran and the exception store returned no "
                        "rows. That is 'the store is empty right now', not "
                        "'I could not read it'."}

    text = output if isinstance(output, str) else json.dumps(output, default=str)

    if any(marker in text for marker in ("│", "+---", "|---")):
        return {**result, "shape": "text", "raw": text[:2000],
                "note": "the console returned a formatted table rather than "
                        "structured rows. The text is in `raw` - read the "
                        "rejection reason out of it directly."}

    handle = parse_query_handle(text)
    if handle:
        return {**result, "shape": "handle", "raw": text[:600],
                "query": handle,
                "note": f"the console accepted the query and returned a "
                        f"HANDLE rather than rows — Striim compiled it into "
                        f"an ad-hoc query application "
                        f"('{handle['query_name']}') and the results are "
                        f"fetched separately, through an endpoint this agent "
                        f"has not found yet. Run this in the Striim console "
                        f"to see the rows:  {statement}"}

    return {**result, "shape": "unknown", "raw": text[:1500],
            "note": f"the console answered, but not with anything this tool "
                    f"recognises as rows. The response is in `raw`. Run this "
                    f"in the Striim console to see it yourself:  {statement}"}


# Words that carry the actual cause in a rejection message. Ordered, so the
# most specific wins when a message contains several.
_REJECTION_HINTS = (
    ("duplicate key", "a duplicate key - the row is already in the target"),
    ("unique constraint", "a unique constraint - the row is already there"),
    ("violates foreign key", "a missing referenced row in another table"),
    ("not-null", "a required column arriving empty"),
    ("null value in column", "a required column arriving empty"),
    ("invalid input syntax", "a value that does not fit the column's type"),
    ("does not exist", "something the writer expects being absent"),
    ("permission denied", "the target refusing this user"),
    ("generated always", "an identity column refusing an explicit value"),
)


def _summarise_rejections(rows):
    """One plain sentence naming the cause, for the incident."""
    blob = json.dumps(rows, default=str).lower()

    cause = next((plain for marker, plain in _REJECTION_HINTS
                  if marker in blob), None)

    lead = f"{len(rows)} rejected row(s) read from the exception store"
    if cause:
        return (f"{lead}. The target refused them because of {cause}. The "
                f"full rows are here - name the specific value in the "
                f"incident, and never tell a human to go and look them up.")
    return (f"{lead}. The reason is in the rows themselves - read it out and "
            f"name it specifically in the incident.")


def _send(striim, statement, timeout=30):
    """The console transport. Does NOT check the verb - callers do that.

    The endpoint is fussy in a way the docs do not mention: it wants a
    plain-text body and an Authorization header of the form
    "token <value>". JSON gets 415; a STRIIM-TOKEN header gets 401.
    """
    if striim.is_offline:
        canned = (striim._fixture.get("console") or {}).get(statement.strip())
        if canned is None:
            raise KeyError(f"no canned console response for: {statement!r}")
        return canned

    def post():
        return requests.post(
            f"{STRIIM_URL}/api/v2/tungsten",
            headers={"Authorization": f"token {striim.token}",
                     "Content-Type": "text/plain"},
            data=statement,
            timeout=timeout,
        )

    response = post()
    if response.status_code in (401, 403):
        striim.login()
        response = post()
    response.raise_for_status()
    return response.json()


def console(striim, statement):
    """Run a READ-ONLY console command and return the parsed response."""
    verb = statement.strip().split(None, 1)[0].upper() if statement.strip() else ""
    if verb not in READ_ONLY_VERBS:
        raise NotReadOnly(
            f"'{verb}' is not a read-only verb. This module cannot change "
            f"anything; write commands go through striim_actions.py."
        )
    return _send(striim, statement)


def _parse_console(raw, statement):
    """Whatever the console said, in a shape the model can read.

    Deliberately shape-agnostic. The only console responses ever CAPTURED
    from this endpoint are DESCRIBE, which come back as structured JSON:

        [{"command": ..., "executionStatus": "Success",
          "output": [...], "responseCode": 200}]

    Striim's interactive CLI renders MON and SHOW as box-drawing tables,
    but that is a different surface and there is no evidence this endpoint
    does the same. So: pass structured output through, hand back text as
    text, and say which happened. A parser for tables is not written until
    a response is seen that needs one - writing it now would mean writing
    it against a fixture we invented, which is the mistake FINDINGS 11-16
    exists to record.
    """
    try:
        entry = raw[0] if isinstance(raw, list) else raw
        output = entry.get("output")
        status = entry.get("executionStatus")
    except (AttributeError, IndexError, KeyError, TypeError):
        return {"statement": statement, "shape": "unknown",
                "raw": str(raw)[:1500],
                "note": "the console answered in an unrecognised shape"}

    if status and status != "Success":
        return {"statement": statement, "shape": "error", "status": status,
                "raw": str(entry)[:800],
                "note": f"the console refused this ({status}). To see it "
                        f"yourself, run in the Striim console:  {statement}"}

    if isinstance(output, list) and all(isinstance(o, dict) for o in output):
        return {"statement": statement, "shape": "structured",
                "output": output[:50], "note": None}

    text = output if isinstance(output, str) else json.dumps(output, default=str)
    return {"statement": statement, "shape": "text", "raw": text[:4000],
            "note": ("the console returned formatted text rather than "
                     "structured rows - read it directly"
                     if any(m in text for m in ("│", "+---", "|---"))
                     else None)}


def console_read(striim, statement):
    """Run any READ-ONLY console command. The escape hatch.

    For the questions nobody anticipated. The leading verb is allowlisted -
    everything else goes through striim_actions.py, where the namespace
    lock and the audit log live.

    SELECT is refused here even though it reads, because on this cluster it
    compiles into a transient application in the Global namespace. That is
    a write, outside the namespace this agent is locked to, and it is not
    something a model should be able to reach for.
    """
    statement = (statement or "").strip()
    if not statement.endswith(";"):
        statement += ";"

    verb = statement.split(None, 1)[0].upper() if statement else ""

    if verb == "SELECT":
        return {"statement": statement, "shape": "refused",
                "note": "SELECT is not available here: on this cluster it "
                        "creates a transient application in the Global "
                        "namespace, which is a write and is outside the "
                        "namespace this agent may act in. To read rejected "
                        "rows use get_rejected_rows, which builds its own "
                        "statement against a store it has verified."}

    if verb not in READ_ONLY_VERBS:
        return {"statement": statement, "shape": "refused",
                "note": f"'{verb}' is not a read. Only "
                        f"{', '.join(sorted(READ_ONLY_VERBS))} may be run "
                        f"here; anything that changes something goes through "
                        f"the action layer, where the namespace lock and the "
                        f"audit log are."}

    try:
        return _parse_console(_send(striim, statement), statement)
    except Exception as error:
        return {"statement": statement, "shape": "error",
                "error": f"{type(error).__name__}: {error}",
                "note": f"could not run it. In the Striim console:  "
                        f"{statement}"}


def show_checkpoint(striim, app):
    """Is the position ADVANCING, or merely present?

    get_checkpoints answers "does a checkpoint exist" and cannot answer
    this - the health record carries checkpoint times but never a position,
    and a stuck pipeline writes checkpoints at the same offset forever.
    """
    result = console_read(striim, f"SHOW {app} CHECKPOINT HISTORY;")
    result["app"] = app
    if result.get("shape") in ("structured", "text"):
        result["note"] = (result.get("note") or "") + (
            " Compare the positions across entries: times advancing while "
            "the position does not means the pipeline is checkpointing and "
            "reading nothing.")
    return result


def mon_app(striim, app):
    """Live per-component rates and states, straight from the cluster."""
    result = console_read(striim, f"MON {app};")
    result["app"] = app
    return result


# Whatever MON calls them. The response shape from this endpoint has never
# been captured (FINDINGS section 19), so rather than assume a key name,
# every metric is matched by what it MEANS and reported honestly when
# nothing matches.
_DDL_KEYS = {
    "ignored": ("ddlsignored", "ignoredddl", "ddlignored", "ddlsskipped",
                "skippedddl", "ddlignoredcount"),
    "captured": ("lastddl", "lastcaptureddl", "lastcapturedddl",
                 "lastddlcaptured", "lastddlstatement"),
    "at": ("lastddltime", "lastddltimestamp", "ddltimestamp",
           "lastddlcapturedtime", "lastddlat"),
}


def ddl_metrics(striim, app):
    """What the source's schema has done, and what was NOT forwarded.

    With schema evolution set to Ignore, a DDL on the source is captured
    and then deliberately dropped. Nothing breaks. The target simply falls
    behind, silently, until some write happens to need the column that was
    never added - which can be hours later and looks like an unrelated
    fault when it arrives.

    The ignored-DDL counter ticks the MOMENT somebody alters the source.
    That is the earliest warning available anywhere in the system, and it
    costs one console call.
    """
    result = mon_app(striim, app)
    found = {"app": app, "shape": result.get("shape"),
             "statement": result.get("statement"),
             "ignored_ddl_count": None, "last_ddl_statement": None,
             "last_ddl_at": None}

    flat = {}
    if result.get("shape") == "structured":
        for entry in result.get("output") or []:
            for key, value in (entry or {}).items():
                flat[re.sub(r"[^a-z0-9]", "", str(key).lower())] = value

    for field, names in _DDL_KEYS.items():
        for name in names:
            if name in flat and flat[name] not in (None, ""):
                found[{"ignored": "ignored_ddl_count",
                       "captured": "last_ddl_statement",
                       "at": "last_ddl_at"}[field]] = flat[name]
                break

    count = found["ignored_ddl_count"]
    try:
        count = int(count) if count is not None else None
        found["ignored_ddl_count"] = count
    except (TypeError, ValueError):
        pass

    if not flat:
        found["note"] = (
            f"could not read DDL metrics: MON came back as "
            f"{result.get('shape')}, not as structured rows. This is 'I "
            f"could not look', NOT 'no DDL has been ignored'. Run "
            f"`{result.get('statement')}` in the console to see them.")
    elif count:
        found["note"] = (
            f"{count} DDL statement(s) have been captured from the source "
            f"and NOT applied to the target. Schema evolution is set to "
            f"ignore them, so the target is behind the source by that many "
            f"changes and nothing has broken yet - it will break on the "
            f"first write that needs one of them."
            + (f" The last was: {found['last_ddl_statement']}"
               if found["last_ddl_statement"] else ""))
    elif count == 0:
        found["note"] = ("no DDL has been captured and ignored - the target's "
                         "shape has not been left behind by a source change")
    else:
        found["note"] = ("MON answered but reported no DDL counters. Either "
                         "this connector does not publish them or none has "
                         "occurred; this cannot tell which.")

    return found


def mon_component(striim, component):
    """The same, for one component - is THIS the bottleneck?"""
    result = console_read(striim, f"MON {component};")
    result["component"] = component
    return result


def _plausible_offset(offset, source_name):
    """Can this recorded read position still be sought to?

    Observed on the real cluster: recovery enabled, checkpoints written with
    real timestamps, has_usable_checkpoint true - and a recorded read offset
    of 170,969 bytes on a file of about 1KB. The position had accumulated
    across successive replacements of the file and no longer referred to the
    file that was actually there. Striim cannot seek to it, so it re-reads
    from the start and duplicates everything into the target.

    The honest answer is usually "I cannot tell from here". Striim reports
    the position but not the size of the file it points into, so a number
    on its own proves nothing - 170,969 is perfectly ordinary on a 2MB file
    and catastrophic on a 1KB one. Resist the temptation to invent a
    threshold: it would pass the case above and then fail somewhere real.

    What DOES settle it is whether the named file is still the same file,
    and that is answered by the error text rather than by arithmetic - see
    duplication_risk() in nodes.py.

    Returns (verdict, explanation) where verdict is True, False or None.
    """
    try:
        value = int(offset)
    except (TypeError, ValueError):
        return None, ("the recorded read position is not a number, so it "
                      "cannot be checked from here")

    if value <= 0:
        return True, ("the recorded read position is at the start of the "
                      "source, so there is nothing to seek to and nothing "
                      "to re-read")

    return None, (
        f"the recorded read position is {value:,} bytes into "
        f"{source_name or 'the source'}. Whether that is still a valid place "
        "to resume depends on whether this is the same file it was written "
        "against - Striim does not report the file's size, so this cannot be "
        "settled by the number alone. If the source has been replaced or "
        "removed, the position is meaningless and a restart re-reads "
        "everything. Say that plainly rather than assuming it is fine.")


def describe_component(striim, component):
    """What a component is CONFIGURED to do - and where it got to.

    This answers the question the health record cannot: not "the source
    failed" but "the source is looking for AI.csv in UploadedFiles/, and it
    had read up to byte 170,969 of it". That turns "check the source
    system" into a named file and a position, which is the difference
    between an incident someone can act on and one they cannot.
    """
    try:
        raw = console(striim, f"DESCRIBE {component};")
    except Exception as error:
        return {"component": component,
                "error": f"{type(error).__name__}: {error}",
                "note": "could not read the component configuration. This is "
                        "'I could not look', not 'there is nothing there'."}

    try:
        entry = raw[0]["output"][0]
    except (KeyError, IndexError, TypeError):
        return {"component": component, "raw": str(raw)[:800],
                "note": "unexpected response shape - could not parse the "
                        "component configuration"}

    props = entry.get("properties", {}) or {}
    checkpoint = (entry.get("Checkpoint") or [{}])[0]
    position = (checkpoint.get("Source Current Position")
                or checkpoint.get("Source Restart Position") or {})

    # Property keys come back lower-cased from the real endpoint
    # ("directory", "wildcard", "filename"), whatever case they were
    # written in, so normalise before looking anything up.
    lowered = {str(k).lower(): v for k, v in props.items()}

    expects = None
    if lowered.get("directory") or lowered.get("wildcard"):
        expects = f"{lowered.get('directory', '')}{lowered.get('wildcard', '')}"
    elif lowered.get("filename"):
        expects = lowered["filename"]
    elif lowered.get("tables"):
        expects = lowered["tables"]
    elif lowered.get("topic"):
        expects = lowered["topic"]

    adapter = entry.get("adapterName")
    read_position = None
    offset_note = None

    if position:
        offset_end = _bytes(position.get("Offset End"))
        offset_begin = _bytes(position.get("Offset Begin"))
        plausible, offset_note = _plausible_offset(offset_end,
                                                   position.get("Source Name"))
        read_position = {
            "source_name": position.get("Source Name"),
            "offset_begin_bytes": offset_begin,
            "offset_end_bytes": offset_end,
            "record_length": _bytes(position.get("Record Length")),
            "position_created": position.get("Creation Time"),
            "offset_looks_plausible": plausible,
            "note": offset_note,
        }

    # SOURCE or TARGET, straight from the response. Several notes below are
    # only true of one of them, and a note that is true of a source is
    # actively misleading on a target - a writer has no read position, so
    # saying "a restart begins from the beginning" about one describes a
    # risk that does not exist and sends a reader looking for it.
    role = str(entry.get("entityType") or "").upper()

    notes = []
    if expects:
        notes.append(
            f"this component is configured to {'write to' if role == 'TARGET' else 'read'} "
            f"{expects}. If that is missing or unreachable, that is the "
            f"fault - name it directly rather than saying 'check the "
            f"{'target' if role == 'TARGET' else 'source'} system'.")
    if offset_note:
        notes.append(offset_note)
    if not position and role != "TARGET":
        notes.append("no read position recorded for this component - that is "
                     "'no position stored', which for a source means a "
                     "restart begins from the beginning.")

    return {
        "component": component,
        "adapter": adapter,
        "role": role.lower() or None,
        "created": entry.get("created"),
        "expects": expects,
        "properties": props,
        "profile": profile_for(component, adapter=adapter),
        "read_position": read_position,
        "note": " ".join(notes) or None,
    }


def preflight(striim, app):
    """Misconfigurations that were wrong from the moment it was created.

    Two of the faults hit during setup were never runtime failures - they
    were present from creation and would have been visible before the
    pipeline was ever deployed. This reports; it never blocks.

    The self-replication check is the one that pays for the rest. A target
    configured with a wildcard, or with the source table in its own table
    list, writes its output back into its own input. The pipeline then
    looks busy forever and the row count climbs without limit, and nothing
    in the control plane calls that a fault.
    """
    findings = []
    flow = get_flow_stats(striim, app)

    source_component = (flow.get("source") or {}).get("component")
    target_component = (flow.get("target") or {}).get("component")

    source_tables = _tables_of(striim, source_component)
    target_tables = _tables_of(striim, target_component)

    if target_tables and any("%" in t or "*" in t for t in target_tables):
        findings.append({
            "check": "table_mapping",
            "severity": "high",
            "note": f"the target is configured with a wildcard table mapping "
                    f"({', '.join(target_tables)}). A wildcard target can "
                    f"match the pipeline's own source table, in which case it "
                    f"writes its output back into its own input and loops. "
                    f"Set the tables explicitly.",
        })

    overlap = sorted(set(source_tables) & set(target_tables))
    if overlap:
        findings.append({
            "check": "self_replication",
            "severity": "high",
            "note": f"the source and the target are both configured for "
                    f"{', '.join(overlap)}. Everything read is written back "
                    f"to where it was read from - this pipeline feeds itself.",
        })

    findings += _batch_policy_findings(striim, target_component, flow)

    checkpoints = get_checkpoints(striim, app)
    behaviour = ((flow.get("source") or {}).get("profile") or {}).get("behaviour")
    if behaviour in ("cdc_reader", "stream_reader") and not checkpoints.get(
            "recovery_enabled"):
        findings.append({
            "check": "recovery",
            "severity": "high",
            "note": "this is a change-capture source with recovery disabled. "
                    "Any restart resumes from now and silently skips whatever "
                    "happened while it was down. Recovery can only be set "
                    "when the application is created.",
        })

    rejected = get_rejected_events(striim, app)
    if rejected.get("exception_store_exists") is False:
        findings.append({
            "check": "exception_store",
            "severity": "medium",
            "note": "there is no exception store, so if the target starts "
                    "refusing rows there will be no record of which ones. "
                    "Set with USE EXCEPTIONSTORE at creation.",
        })

    return {
        "app": app,
        "checks_run": ["table_mapping", "self_replication", "batch_policy",
                       "recovery", "exception_store"],
        "findings": findings,
        "clean": not findings,
        "note": ("nothing to flag before this runs"
                 if not findings else
                 f"{len(findings)} thing(s) worth fixing before this runs "
                 f"properly. None of them will show up as a fault."),
    }


_POLICY_PART = re.compile(r"([A-Za-z]+)\s*:\s*(\d+)\s*([A-Za-z]*)")

# Above this many events, a batch is big enough that a low-volume pipeline
# will sit below it indefinitely.
_BIG_BATCH = 100


def _batch_policy_findings(striim, component, flow):
    """Will a trickle of rows ever actually get written?

    Observed live: `EventCount:1000,Interval:60`. A pipeline moving a few
    rows a minute never reaches 1000, so everything it reads sits in a
    buffer, and Striim reports the app as perfectly healthy while the
    target stays empty. There is no fault, no alert and no discarded
    event - just events in and nothing out.

    The interval is what saves you, so the dangerous case is a policy with
    a large event count and no interval at all.
    """
    if not component:
        return []

    described = describe_component(striim, component)
    props = {str(k).lower(): v for k, v in
             (described.get("properties") or {}).items()}
    raw = str(props.get("batchpolicy") or props.get("uploadpolicy")
              or props.get("flushpolicy") or "")
    if not raw:
        return []

    parts = {name.lower(): (int(number), unit.lower())
             for name, number, unit in _POLICY_PART.findall(raw)}
    events = parts.get("eventcount", (None, ""))[0]
    interval, unit = parts.get("interval", (None, ""))

    source_rate = (flow.get("source") or {}).get("event_rate")
    low_volume = source_rate is not None and source_rate < 10

    if events and interval is None:
        return [{
            "check": "batch_policy",
            "severity": "high",
            "note": f"the target flushes only every {events:,} events "
                    f"({raw}) and has no time-based interval. A pipeline "
                    f"moving fewer than that will buffer forever: events go "
                    f"in, nothing comes out, and Striim reports it healthy "
                    f"the whole time. Add an interval.",
        }]

    if events and events >= _BIG_BATCH:
        seconds = interval if unit in ("", "s", "sec", "secs") else None
        wait = (f"up to {seconds}s" if seconds
                else f"a full interval ({raw})")
        severity = "high" if low_volume else "medium"
        measured = (f" The source is currently moving {source_rate:g} "
                    f"events/second, so it will never reach {events:,} - "
                    f"every write will wait for the timer."
                    if low_volume else "")
        return [{
            "check": "batch_policy",
            "severity": severity,
            "note": f"the target buffers {events:,} events or {wait} before "
                    f"writing ({raw}). On a low-volume pipeline rows sit "
                    f"invisible in the buffer for that long and the target "
                    f"looks empty.{measured}",
        }]

    return []


def _tables_of(striim, component):
    """The table list a component is configured for, lower-cased."""
    if not component:
        return []
    described = describe_component(striim, component)
    props = {str(k).lower(): v for k, v in (described.get("properties") or {}).items()}
    raw = str(props.get("tables") or "")
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def describe_pipeline(striim, app):
    """Everything at once. A cheap first call when you know nothing yet."""
    return {
        "status": get_status(striim, app),
        "flow": get_flow_stats(striim, app),
        "errors": get_errors(striim, app),
        "history": get_state_history(striim, app),
        "checkpoints": get_checkpoints(striim, app),
        "rejected_events": get_rejected_events(striim, app),
    }


# ============================================================== REGISTRY

TOOLS = {
    "list_apps": list_apps,
    "get_status": get_status,
    "get_errors": get_errors,
    "get_flow_stats": get_flow_stats,
    "get_state_history": get_state_history,
    "get_checkpoints": get_checkpoints,
    "get_rejected_events": get_rejected_events,
    "get_rejected_rows": get_rejected_rows,
    "describe_component": describe_component,
    "describe_pipeline": describe_pipeline,
    "console_read": console_read,
    "show_checkpoint": show_checkpoint,
    "mon_app": mon_app,
    "mon_component": mon_component,
    "ddl_metrics": ddl_metrics,
    "preflight": preflight,
    # Lives in db.py but takes a Striim client, because discovery reads the
    # pipeline's own component configuration to find its databases.
    "db_config": _db.db_config,
}
