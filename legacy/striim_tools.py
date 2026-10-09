"""
Striim tool layer
=================

The agent's senses. Every tool here is READ ONLY - nothing in this file
can change anything on the cluster.

Two different safety rules, deliberately:
  READS  - allowed anywhere. Diagnosing someone else's pipeline is harmless
           and it proves the agent is not file-specific.
  WRITES - live in a different module and are restricted to MY_NAMESPACE.

Test any tool on its own:
    python striim_tools.py list_apps
    python striim_tools.py get_status sarah_moolai.Trial3
    python striim_tools.py get_errors sarah_moolai.Trial3
    python striim_tools.py get_flow_stats sarah_moolai.Trial3
    python striim_tools.py get_state_history sarah_moolai.Trial3
    python striim_tools.py get_checkpoints sarah_moolai.Trial3
    python striim_tools.py describe_pipeline sarah_moolai.Trial3
"""

import json
import os
import time

import requests

STRIIM_URL = os.environ.get("STRIIM_URL", "https://developer.striim.com")
MY_NAMESPACE = os.environ.get("STRIIM_NAMESPACE", "sarah_moolai")

FAULT_STATES = {"HALT", "TERMINATED", "CRASH"}
CALM_STATES = {"RUNNING", "COMPLETED", "STOPPED", "QUIESCED",
               "CREATED", "DEPLOYED"}


# ===================================================== CONNECTOR PROFILES

# What "normal" looks like for each kind of source. The model uses this to
# avoid treating a quiet file reader like a dead CDC stream.
# Extending the agent to a new connector means adding lines here, not code.
PROFILES = {
    "FileReader": {
        "silence_is_normal": True,
        "why": "reads a file to the end, then idles with nothing to do",
        # State the CONDITION, not the conclusion. A flat "HIGH RISK" here
        # drowns out the checkpoint evidence from get_checkpoints - the
        # model repeats the loudest tool instead of reconciling the two.
        "restart_risk": "DEPENDS ON THE CHECKPOINT. Call get_checkpoints "
                        "before judging this. If has_usable_checkpoint is "
                        "false, a restart re-reads the whole file and "
                        "duplicates every row into the target - high risk. "
                        "If it is true, the reader resumes from its saved "
                        "position and a restart is reasonably safe.",
    },
    "OracleReader": {
        "silence_is_normal": False,
        "why": "change capture on a live database; long silence is suspicious",
        "restart_risk": "resumes from stored position if recovery is on - "
                        "check the checkpoint before restarting",
    },
    "PostgresReader": {
        "silence_is_normal": False,
        "why": "change capture on a live database; long silence is suspicious",
        "restart_risk": "replication slot may have advanced or been dropped",
    },
    "MysqlReader": {
        "silence_is_normal": False,
        "why": "change capture on a live database; long silence is suspicious",
        "restart_risk": "binlog may have been purged past the stored position",
    },
    "SalesforceReader": {
        "silence_is_normal": True,
        "why": "polls an API; quiet periods are expected outside business hours",
        "restart_risk": "may re-consume API call quota",
    },
    "BigQueryReader": {
        "silence_is_normal": True,
        "why": "batch query source; runs on a schedule",
        "restart_risk": "may re-run the query and duplicate results",
    },
}


def profile_for(component_name):
    """Guess the connector type from the component name Striim gives it.

    Striim names components after their adapter - 'Trial3_FileReaderSource',
    'X_OracleSource' - so the type is usually sitting in the name.
    """
    if not component_name:
        return None

    lowered = component_name.lower()
    for key, profile in PROFILES.items():
        if key.lower().replace("reader", "") in lowered:
            return {"connector_guess": key, **profile}
    return {"connector_guess": "unknown",
            "silence_is_normal": None,
            "why": "connector type not recognised from the component name",
            "restart_risk": "unknown - treat with caution"}


# ============================================================== TRANSPORT

class Striim:
    """One door to Striim. Everything goes through here."""

    def __init__(self):
        self._cache = None
        self._cached_at = 0
        self.login()

    def login(self):
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

        Cached briefly: a single diagnosis may call five tools, and there is
        no sense fetching the same snapshot five times. /health only refreshes
        every couple of minutes anyway.
        """
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


# ================================================================= TOOLS

def _ago(timestamp_ms):
    if not timestamp_ms:
        return None
    return round((time.time() * 1000 - timestamp_ms) / 1000)


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


def get_status(striim, app):
    """Is it alive, and how hard is it working?"""
    info = striim.health().get("appHealthMap", {}).get(app)
    if info is None:
        return {"app": app, "status": "NOT FOUND",
                "note": "no such application in the health record - check the "
                        "exact name including namespace, or it may be new"}

    status = info.get("status")

    # Zero throughput means different things in different states. Spell it
    # out rather than leaving the model to infer it: an app that never
    # started looks identical to one that finished, and to one that stalled.
    MEANING = {
        "CREATED":   "written but never placed on a machine - has never run",
        "DEPLOYED":  "placed on a machine but NOT STARTED - has never read "
                     "anything. Zero throughput is expected and means nothing.",
        "RUNNING":   "started and live",
        "COMPLETED": "ran and finished its work normally - not a fault",
        "STOPPED":   "switched off deliberately by someone - not a fault",
        "QUIESCED":  "paused cleanly with in-flight data flushed - not a fault",
        "HALT":      "broke because of something OUTSIDE Striim",
        "TERMINATED": "broke because of something INSIDE the pipeline",
    }

    return {
        "app": app,
        "status": status,
        "status_means": MEANING.get(status, "unrecognised state"),
        "has_ever_run": status not in ("CREATED", "DEPLOYED"),
        "is_fault": status in FAULT_STATES,
        "throughput_rate": info.get("rate"),
        "cpu_rate": info.get("cpuRate"),
        # NB: this is when the application DEFINITION was last edited,
        # NOT when it faulted. Do not use it to age a fault.
        "definition_last_edited_seconds_ago": _ago(info.get("lastModifiedTime")),
    }


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

    # Nothing found - but WHY nothing found matters, and the honest answer
    # depends on how old the fault is. Telling the model "try again in a
    # minute" about an app that broke eight months ago sends it down a
    # false path. A tool's wording shapes the reasoning as much as its data.
    #
    # Age the fault from the STATE HISTORY, which carries real transition
    # timestamps. The app record's lastModifiedTime is when the definition
    # was last EDITED - using it here would invent a fault age out of thin air.
    age = None
    for change in record.get("stateChangeList", []):
        if change.get("fqName") == app and change.get("currentStatus") in FAULT_STATES:
            age = _ago(change.get("timestamp"))
            break

    if age is None:
        note = ("no error text found. The age of this fault is unknown - it "
                "has aged out of the recent-transition window - so this could "
                "be a fault whose text is still pending, or an old one that "
                "never recorded any. Do not assume either.")
    elif age < 300:
        note = ("no error text yet - this fault is only "
                f"{age}s old and the text is often published a minute or two "
                "after the status changes. Worth checking again shortly.")
    else:
        hours = round(age / 3600)
        note = (f"no error text, and this fault is about {hours} hours old. "
                "The text is not pending - none was ever recorded, or it has "
                "since been cleared. Do not wait for it; reason from the "
                "other evidence or say plainly that the cause is unavailable.")

    return {
        "app": app,
        "error_count": 0,
        "errors": [],
        "fault_age_seconds": age,
        "note": note,
    }


def get_flow_stats(striim, app):
    """Rates and timestamps for the source and target ends.

    Reads climbing while writes flatline means the target is the problem.
    Both at zero means the source or the app itself.
    """
    record = striim.health()

    def ours(name):
        return name.startswith(app.rsplit(".", 1)[0] + ".") and app.split(".")[-1] in name

    sources = {n: i for n, i in record.get("sourceHealthMap", {}).items() if ours(n)}
    targets = {n: i for n, i in record.get("targetHealthMap", {}).items() if ours(n)}

    src_name, src = next(iter(sources.items()), (None, {}))
    tgt_name, tgt = next(iter(targets.items()), (None, {}))

    return {
        "app": app,
        "source": {
            "component": src_name,
            "event_rate": src.get("eventRate"),
            "seconds_since_last_event": _ago(src.get("lastEventTime")),
            "profile": profile_for(src_name),
        },
        "target": {
            "component": tgt_name,
            "event_rate": tgt.get("eventRate"),
            "seconds_since_last_write": _ago(tgt.get("lastWriteTime")),
            "end_to_end_lag": tgt.get("lagEnd2End"),
        },
    }


def get_state_history(striim, app):
    """Recent state transitions.

    The key question: did a human ask for this? A pipeline stopped on
    purpose passes through STOPPING first. One that broke does not.
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
    # across the whole cluster - anything older has aged out of it.
    # Concluding "not deliberate" from silence is how an agent talks itself
    # into restarting something a human switched off on purpose.
    if not transitions:
        return {
            "app": app,
            "transitions": [],
            "looks_deliberate": None,
            "note": "NO DATA. The state change list is a short rolling window "
                    "and this app's transitions have aged out of it. This tells "
                    "you nothing about whether the change was deliberate - do "
                    "not treat it as evidence either way.",
        }

    latest = changes[-1]
    landed_on = latest.get("currentStatus")
    path = [c.get("currentStatus") for c in changes]

    # The STOPPING test only describes a transition INTO a stopped state.
    # Applying it to a START is a category error: nothing starts itself, so
    # STOPPED -> RUNNING is always deliberate, and the absence of STOPPING
    # there means nothing at all.
    if landed_on in ("RUNNING", "DEPLOYED", "CREATED"):
        return {
            "app": app,
            "transitions": transitions,
            "looks_deliberate": True,
            "note": f"the most recent transition ended in {landed_on}, which "
                    "is a start. Starts are always requested by someone (or by "
                    "auto-resume) - the STOPPING test does not apply here.",
        }

    deliberate = "STOPPING" in path or "QUIESCING" in path

    return {
        "app": app,
        "transitions": transitions,
        "looks_deliberate": deliberate,
        "note": ("passed through STOPPING - a human almost certainly did this"
                 if deliberate else
                 f"reached {landed_on} with no STOPPING transition - nobody "
                 "asked for it, so it broke"),
    }


def get_checkpoints(striim, app):
    """Is the recovery bookmark advancing?

    null means recovery was never enabled on this application - it can only
    be set when the app is created. Without it, every start is a cold start.
    """
    info = striim.health().get("appHealthMap", {}).get(app, {})
    checkpoints = info.get("checkpointInformation")

    if not checkpoints:
        return {
            "app": app,
            "recovery_enabled": False,
            "has_usable_checkpoint": False,
            "note": "no checkpoint data - recovery is not enabled on this "
                    "application, so it cannot resume from a stored position. "
                    "Every start is a cold start.",
        }

    # The structure can exist while every value inside it is "N/A" - recovery
    # switched on, but no checkpoint ever actually written. Reading only the
    # outer structure would say "recovery is on, safe to resume" and be wrong.
    flat = json.dumps(checkpoints)
    recorded = flat.count('"N/A"') == 0

    return {
        "app": app,
        "recovery_enabled": True,
        "has_usable_checkpoint": recorded,
        "checkpoints": checkpoints,
        "note": (None if recorded else
                 "Recovery is enabled but every recorded checkpoint is N/A - "
                 "none was ever written. There is no position to resume from, "
                 "so this behaves like recovery being off."),
    }


def get_rejected_events(striim, app):
    """The exception store - individual events the target refused.

    This is where a poison event actually lives: not "something failed" but
    the specific row that could not be delivered. If rows are landing here
    while the pipeline reports healthy, data is being silently dropped and
    no alert will fire.

    Two levels of answer:
      * whether the store exists and has received anything (always available)
      * the rows themselves (needs a SELECT, which not every deployment
        exposes over REST - reported honestly if unavailable)
    """
    record = striim.health()
    stores = record.get("waStoreHealthMap", {})

    # Striim names it <app>_ExceptionStore.
    expected = f"{app}_ExceptionStore"
    store = stores.get(expected)

    if store is None:
        return {
            "app": app,
            "exception_store_exists": False,
            "note": "no exception store on this application. Rejected events "
                    "are not being captured anywhere, so if the target is "
                    "refusing rows there is no record of which ones. This is "
                    "set with USE EXCEPTIONSTORE when the app is created.",
        }

    last_write = store.get("lastWriteTime")
    count_seen = last_write not in (None, 0)

    return {
        "app": app,
        "exception_store_exists": True,
        "store_name": expected,
        "write_rate": store.get("writeRate"),
        "seconds_since_last_rejection": _ago(last_write) if count_seen else None,
        "has_ever_rejected": count_seen,
        "note": (
            "events HAVE been rejected into the exception store - the target "
            "refused them. Individual rows are not readable through this API; "
            "query the store directly in the console with "
            f"SELECT * FROM {expected}; to see the failing payloads."
            if count_seen else
            "the exception store exists but nothing has ever been rejected "
            "into it. No individual events have been refused by the target."
        ),
    }


def console(striim, statement):
    """Run a read-only console command and return the parsed response.

    The console endpoint is fussy: it wants a plain text body and an
    Authorization header of the form "token <value>". JSON gets 415.
    """
    response = requests.post(
        f"{STRIIM_URL}/api/v2/tungsten",
        headers={"Authorization": f"token {striim.token}",
                 "Content-Type": "text/plain"},
        data=statement,
        timeout=30,
    )
    if response.status_code in (401, 403):
        striim.login()
        return console(striim, statement)
    response.raise_for_status()
    return response.json()


def describe_component(striim, component):
    """What a component is CONFIGURED to do - and where it got to.

    This answers the question the health record cannot: not "the source
    failed" but "the source is looking for AI.csv in UploadedFiles/, and
    it had read up to byte 170,969 of it".

    That turns "check the source system" into a named file and a position.
    """
    try:
        raw = console(striim, f"DESCRIBE {component};")
    except Exception as error:
        return {"component": component,
                "error": f"{type(error).__name__}: {error}",
                "note": "could not read the component configuration"}

    try:
        entry = raw[0]["output"][0]
    except (KeyError, IndexError, TypeError):
        return {"component": component, "raw": str(raw)[:800],
                "note": "unexpected response shape"}

    props = entry.get("properties", {})
    checkpoint = (entry.get("Checkpoint") or [{}])[0]
    position = checkpoint.get("Source Current Position", {})

    expects = None
    if props.get("directory") or props.get("wildcard"):
        expects = f"{props.get('directory', '')}{props.get('wildcard', '')}"

    return {
        "component": component,
        "adapter": entry.get("adapterName"),
        "created": entry.get("created"),
        "expects": expects,
        "properties": props,
        "read_position": {
            "source_name": position.get("Source Name"),
            "offset_begin": position.get("Offset Begin"),
            "offset_end": position.get("Offset End"),
        } if position else None,
        "note": (f"this component is configured to read {expects}. If that is "
                 "missing or unreadable, that is the fault - name it directly "
                 "rather than saying 'check the source system'."
                 if expects else None),
    }


def describe_pipeline(striim, app):
    """Everything at once. Cheap first call when you know nothing yet."""
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
    "describe_component": describe_component,
    "describe_pipeline": describe_pipeline,
}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(__doc__)
        print("Available:", ", ".join(TOOLS))
        sys.exit(1)

    name = sys.argv[1]
    args = sys.argv[2:]

    if name not in TOOLS:
        print(f"No such tool: {name}")
        print("Available:", ", ".join(TOOLS))
        sys.exit(1)

    client = Striim()
    output = TOOLS[name](client, *args)
    print(json.dumps(output, indent=2, default=str))