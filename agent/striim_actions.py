"""
Striim action layer
===================

The ONLY file in this project that can change anything on the cluster.
Everything else reads. Keeping the write path in one small file means the
question "what can this agent actually do?" is answered by reading it.

Five hard limits, enforced here in Python rather than trusted to the model:

  * namespace   refuses to touch anything outside config.NAMESPACE
  * verb list   only START, STOP, RESUME, DEPLOY, UNDEPLOY. Nothing else runs.
  * budget      at most MAX_RESTARTS_PER_HOUR restarts per app per hour
  * no repeats  never acts twice on the same incident fingerprint. A repeat
                is a pattern, and patterns need a person.
  * DRY_RUN     on unless STRIIM_LIVE=1. Prints what it would do, sends nothing.

The first two are about scope; the middle two are about a loop that would
otherwise be invisible until Monday. DECIDE also applies the budget and
repeat rules when it picks a tier - that is deliberate duplication, because
DECIDE is one refactor away from being wrong and this is the last gate.
"""

import json
import re
import time
from datetime import datetime, timezone

import requests

from . import config
from . import striim_tools as st

# Only these verbs. A typo, or a confused model, cannot run anything else.
ALLOWED_VERBS = {"START", "STOP", "RESUME", "DEPLOY", "UNDEPLOY"}


class ActionRefused(Exception):
    """A guardrail blocked something. Always surfaced, never swallowed."""


# ============================================================== LEDGER

class Ledger:
    """What has already been attempted, kept across restarts of the agent.

    In memory only, this resets every time the process dies - which is
    exactly when a crash-looping agent would burn its whole restart budget
    again. So it goes on disk, as one small JSON file.
    """

    def __init__(self, path=None):
        self.path = path or config.LEDGER_FILE
        self.data = {"attempts": [], "fingerprints": {}}
        self._load()

    def _load(self):
        try:
            with open(self.path) as handle:
                self.data = json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        self.data.setdefault("attempts", [])
        self.data.setdefault("fingerprints", {})

    def _save(self):
        config.ensure_state_dir()
        with open(self.path, "w") as handle:
            json.dump(self.data, handle, indent=2)

    # ---------------------------------------------------------- queries

    def restarts_in_last_hour(self, app):
        cutoff = time.time() - 3600
        return [a for a in self.data["attempts"]
                if a["app"] == app and a["at"] > cutoff]

    def budget_left(self, app):
        return config.MAX_RESTARTS_PER_HOUR - len(self.restarts_in_last_hour(app))

    def already_tried(self, fingerprint):
        return fingerprint in self.data["fingerprints"]

    def when_tried(self, fingerprint):
        return self.data["fingerprints"].get(fingerprint)

    def times_seen(self, fingerprint):
        """How many times this exact fault has been acted on before.

        Not the same as already_tried, which is a yes/no gate. This is the
        count, and it is what tells a recurring exception apart from a
        one-off - a skill that proposes ignoring an exception has no
        business firing the first time one appears.
        """
        if not fingerprint:
            return 0
        return sum(1 for a in self.data["attempts"]
                   if a.get("fingerprint") == fingerprint)

    # ---------------------------------------------------------- writes

    def record(self, app, verb, fingerprint, live):
        now = time.time()
        self.data["attempts"].append(
            {"app": app, "verb": verb, "fingerprint": fingerprint,
             "at": now, "live": live}
        )
        if fingerprint:
            self.data["fingerprints"][fingerprint] = now
        # Keep the file from growing without bound; a day is plenty of
        # history for an hourly budget and a same-incident check.
        cutoff = now - 86400
        self.data["attempts"] = [a for a in self.data["attempts"]
                                 if a["at"] > cutoff]
        self._save()

    def clear(self):
        """Used by the tests, and by a human who has fixed the root cause."""
        self.data = {"attempts": [], "fingerprints": {}}
        self._save()


def audit(entry):
    """Append-only record of everything the agent changed, ever.

    Separate from the ledger on purpose: the ledger is state the agent reads
    back and prunes, this is evidence for a person and is never pruned.
    """
    config.ensure_state_dir()
    entry = {"at": datetime.now(timezone.utc).isoformat(), **entry}
    with open(config.AUDIT_FILE, "a") as handle:
        handle.write(json.dumps(entry, default=str) + "\n")
    return entry


# ============================================================ THE GATE

def check(app, verb, fingerprint=None, ledger=None):
    """Every reason this action might be refused, in one place.

    Raises ActionRefused with a sentence a human can read. Returns nothing
    when the action is permitted.
    """
    if verb not in ALLOWED_VERBS:
        raise ActionRefused(f"'{verb}' is not one of the allowed verbs "
                            f"({', '.join(sorted(ALLOWED_VERBS))})")

    if not app.startswith(config.NAMESPACE + "."):
        raise ActionRefused(
            f"{app} is outside {config.NAMESPACE}. This agent may READ any "
            f"pipeline on the cluster but may only ACT on its own namespace."
        )

    if ledger is None:
        return

    if fingerprint and ledger.already_tried(fingerprint):
        raise ActionRefused(
            "this exact fault has already been acted on once and came back. "
            "A repeat is a pattern, not a blip - a person needs to look."
        )

    left = ledger.budget_left(app)
    if left <= 0:
        raise ActionRefused(
            f"restart budget spent for {app.split('.')[-1]} "
            f"({config.MAX_RESTARTS_PER_HOUR} in the last hour). Something is "
            "wrong that restarting is not fixing."
        )


def run_command(striim, app, verb, fingerprint=None, ledger=None):
    """Send one console command. The single point where anything changes."""
    verb = verb.upper()
    check(app, verb, fingerprint, ledger)

    statement = f"{verb} {app};"

    # Offline: the fixture decides what happens next. Checked before
    # DRY_RUN so the acceptance tests can exercise the whole ACT -> VERIFY
    # path, including the case where the restart does not hold.
    if getattr(striim, "is_offline", False):
        striim.acted = True
        striim.invalidate()
        if ledger is not None:
            ledger.record(app, verb, fingerprint, live=False)
        return {"dry_run": False, "offline": True, "statement": statement,
                "note": "offline fixture - no command was sent anywhere"}

    if config.DRY_RUN:
        if ledger is not None:
            ledger.record(app, verb, fingerprint, live=False)
        audit({"app": app, "verb": verb, "statement": statement,
               "fingerprint": fingerprint, "dry_run": True})
        return {"dry_run": True, "statement": statement,
                "note": "set STRIIM_LIVE=1 to actually send this"}

    # The endpoint wants a PLAIN TEXT body and an Authorization header of the
    # form "token <value>". Sending JSON gets 415; sending a STRIIM-TOKEN
    # header gets 401. Both fail quietly enough to look like the command
    # simply not working, which is how this cost an afternoon.
    def send():
        return requests.post(
            f"{config.STRIIM_URL}/api/v2/tungsten",
            headers={"Authorization": f"token {striim.token}",
                     "Content-Type": "text/plain"},
            data=statement,
            timeout=60,
        )

    response = send()
    if response.status_code in (401, 403):
        striim.login()
        response = send()

    # Record BEFORE raising for status. An attempt that failed still counts
    # against the budget - otherwise a command that errors every time can be
    # retried forever.
    if ledger is not None:
        ledger.record(app, verb, fingerprint, live=True)
    audit({"app": app, "verb": verb, "statement": statement,
           "fingerprint": fingerprint, "dry_run": False,
           "http_status": response.status_code,
           "response": response.text[:500]})

    response.raise_for_status()
    striim.invalidate()
    return {"dry_run": False, "statement": statement,
            "response": response.text[:500]}


# ================================================ PROPERTY CHANGES

# A property name and a value the agent is willing to put in a statement.
# Anything else stops here rather than being interpolated into TQL.
_PROPERTY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VALUE = re.compile(r"^[A-Za-z0-9_,.:%/\\ =+-]*$")

# Changing a property means taking the application apart and putting it
# back together. These are the steps, in order, and any of them can fail.
ALTER_CYCLE = ("STOP", "UNDEPLOY", "ALTER", "DEPLOY", "START")


def alter_component_property(striim, app, component, prop, value,
                             evidence=None, ledger=None, verbose=True):
    """Change one component property, and put the pipeline back.

    Five commands with failure possible at each, wrapped so the caller gets
    one answer that says which step failed and what state things were left
    in.

    THE DANGEROUS STEP IS UNDEPLOY, and it is why most of this function is
    guards rather than commands. Striim's recovery state is tied to the
    deployment. If undeploying discards it, the START at the end of this
    cycle is a COLD START - and for a file, batch or spreadsheet source a
    cold start re-reads everything and appends it to the target again,
    silently, which is the single failure this whole project exists to
    prevent.

    I could not confirm Striim's semantics here from any documentation
    available to me, so this does not rely on knowing them. It OBSERVES
    instead:

      * refuses outright when the source is a re-read connector sitting at
        a non-zero position - proposing the five statements instead, which
        a human can run with their eyes open
      * snapshots the recorded position before touching anything
      * re-reads it after DEPLOY and ABORTS BEFORE START if it moved or
        vanished, leaving the application deployed and stopped rather than
        cold-starting it
      * never exits undeployed: if a step fails, it tries to DEPLOY back

    That works whether or not my understanding of UNDEPLOY is correct,
    which is the only kind of guard worth having here.
    """
    from . import striim_tools as st

    if not _PROPERTY.match(str(prop or "")):
        raise ActionRefused(f"{prop!r} is not a plain property name")
    if not _VALUE.match(str(value or "")):
        raise ActionRefused(
            f"{value!r} contains characters this agent will not put into a "
            f"statement. Propose it for a human instead.")
    for name in (app, component):
        if not str(name).startswith(config.NAMESPACE + "."):
            raise ActionRefused(
                f"{name} is outside {config.NAMESPACE} — this agent may read "
                f"any pipeline on the cluster but only change its own.")

    # --- would putting it back be a cold start? ------------------------ #
    refusal = _cold_start_risk(evidence)
    if refusal:
        return {"ok": False, "refused": refusal, "steps": [],
                "statements": _cycle_statements(app, component, prop, value),
                "note": "not run. The statements are above for a human to "
                        "run deliberately."}

    before = _recorded_position(striim, component)

    result = {"ok": False, "steps": [], "position_before": before,
              "old_value": _current_value(striim, component, prop),
              "statements": _cycle_statements(app, component, prop, value)}

    if config.DRY_RUN:
        result.update(ok=True, dry_run=True,
                      note="dry run — none of these were sent")
        return result

    for step, statement in zip(ALTER_CYCLE, result["statements"]):
        # The one place the cycle stops of its own accord: after DEPLOY,
        # before START, if the position is not what it was.
        if step == "START":
            after = _recorded_position(striim, component)
            result["position_after"] = after
            if _position_lost(before, after):
                result.update(
                    ok=False, failed_at="START (not attempted)",
                    note=f"ALTERED and redeployed, but the recorded read "
                         f"position went from {before} to {after}. Starting "
                         f"now would re-read from the beginning. The "
                         f"application is deployed and stopped — run "
                         f"`{statement}` yourself only if re-reading is "
                         f"acceptable.")
                return result

        try:
            _run_statement(striim, statement, verbose=verbose)
            result["steps"].append({"step": step, "ok": True})
        except Exception as error:
            result["steps"].append({"step": step, "ok": False,
                                    "error": str(error)})
            result["failed_at"] = step
            result["note"] = f"{step} failed: {_first_line(error)}"
            _restore(striim, app, step, result, verbose)
            return result

    if ledger is not None:
        ledger.record(app, "ALTER", None, live=True)

    result.update(ok=True, already_restarted=True,
                  note=f"{prop} changed from {result['old_value']!r} to "
                       f"{value!r}; the application was redeployed and "
                       f"started.")
    return result


def _cycle_statements(app, component, prop, value):
    return [f"STOP {app};",
            f"UNDEPLOY {app};",
            f"ALTER {component} SET {prop} = '{value}';",
            f"DEPLOY {app};",
            f"START {app};"]


def _cold_start_risk(evidence):
    """Refuse the cycle where putting it back could re-read everything."""
    if not evidence:
        return ("no evidence was gathered, so whether a redeploy would "
                "re-read this source cannot be judged")

    source = (evidence.get("flow") or {}).get("source") or {}
    behaviour = (source.get("profile") or {}).get("behaviour")
    if behaviour not in ("file_reader", "batch_reader", "spreadsheet_reader"):
        return None

    position = ((evidence.get("source_config") or {})
                .get("read_position") or {})
    offset = position.get("offset_end_bytes")
    if offset:
        return (f"this source has read to byte {offset:,} and undeploying "
                f"may discard that position. If it does, restarting re-reads "
                f"everything and duplicates it into the target.")
    return None


def _recorded_position(striim, component):
    from . import striim_tools as st
    try:
        described = st.describe_component(striim, component)
    except Exception:
        return None
    return ((described.get("read_position") or {}).get("offset_end_bytes"))


def _position_lost(before, after):
    """Did the redeploy throw the position away?"""
    if not before:
        return False              # nothing to lose
    return after is None or after == 0 or after < before


def _current_value(striim, component, prop):
    from . import striim_tools as st
    try:
        props = st.describe_component(striim, component).get("properties") or {}
    except Exception:
        return None
    return {str(k).lower(): v for k, v in props.items()}.get(str(prop).lower())


def _restore(striim, app, failed_at, result, verbose):
    """Never leave the application undeployed.

    An app left in DEPLOYED or CREATED is in CALM_STATES, so the watcher
    treats it as "not a fault" and says nothing about it - forever. The
    agent would break the pipeline and then go quiet.
    """
    if failed_at in ("STOP", "START"):
        return
    try:
        _run_statement(striim, f"DEPLOY {app};", verbose=verbose)
        result["restored"] = True
        result["note"] += " The application was redeployed to avoid leaving "\
                          "it undeployed; it is stopped."
    except Exception as error:
        result["restored"] = False
        result["note"] += (f" WARNING: it could not be redeployed either "
                           f"({_first_line(error)}) — this application is "
                           f"UNDEPLOYED and needs a person now.")


def _run_statement(striim, statement, verbose=True):
    """Send one console statement built by this module. Always audited."""
    if verbose:
        print(f"    -> {statement}")

    if getattr(striim, "is_offline", False):
        striim.acted = True
        striim.invalidate()
        audit({"statement": statement, "offline": True})
        return {"offline": True, "statement": statement}

    from . import striim_tools as st
    audit({"statement": statement, "dry_run": False})
    response = st._send(striim, statement, timeout=60)
    striim.invalidate()
    return {"statement": statement, "response": str(response)[:400]}


def _first_line(error):
    return str(error).strip().splitlines()[0][:160]


# ============================================================== VERIFY

def snapshot(striim, app, position=None):
    """The few numbers that prove whether data is actually moving.

    Taken BEFORE acting. Without a baseline there is nothing to compare
    against afterwards, and "it says RUNNING" is not evidence of anything.

    `position` is where the reader had actually got to - an LSN, an offset.
    It is passed in rather than read here because the authority for it is
    the replication slot, which lives on the far side of the database
    layer. It matters more than everything else in this dict put together:
    see verify().
    """
    striim.invalidate()          # a cached snapshot answers the wrong question
    flow = st.get_flow_stats(striim, app)
    status = st.get_status(striim, app)
    checkpoints = st.get_checkpoints(striim, app)

    return {
        "at": time.time(),
        "status": status["status"],
        "position": position,
        "target_seconds_since_write": flow["target"]["seconds_since_last_write"],
        "source_seconds_since_event": flow["source"]["seconds_since_last_event"],
        "source_behaviour": (flow["source"]["profile"] or {}).get("behaviour"),
        "checkpoint_fingerprint": json.dumps(checkpoints.get("checkpoints"),
                                             sort_keys=True),
    }


def verify(striim, app, before, wait=None, verbose=True, position_now=None):
    """Did the fix actually work?

    "It says RUNNING" is not proof. A pipeline can report RUNNING while
    doing nothing at all, or fail again forty seconds later. Three
    questions, of which only the first is visible in the status field and
    it is the weakest of the three:

      1. Is it in a healthy state at all?
      2. Has the target written something SINCE the action?
      3. Has the checkpoint moved?

    THREE outcomes, not two:
      True   recovered, with evidence of movement
      False  still broken - escalate
      None   healthy, but nothing has moved yet, so this cannot be confirmed

    None is common and correct. A file source that has already read
    everything will sit perfectly healthy with nothing left to prove. An
    agent that cannot say "I don't know" ends up saying something false.
    """
    wait = config.VERIFY_WAIT_SECONDS if wait is None else wait

    # /health is a cached snapshot that lags roughly two minutes. Reading it
    # sooner than that returns the pre-action world and calls a working fix
    # a failure.
    if wait:
        if verbose:
            print(f"    waiting {wait}s for monitoring to catch up "
                  "(/health lags ~2 minutes)...")
        time.sleep(wait)

    after = snapshot(striim, app,
                     position=position_now() if callable(position_now) else None)

    healthy = after["status"] not in st.FAULT_STATES

    # THE POSITION IS THE ANSWER, where there is one.
    #
    # A checkpoint being written is not the same claim as a checkpoint
    # being written somewhere new, and the checkpoint fingerprint below
    # cannot tell them apart - it is a hash of the whole checkpoint record,
    # timestamps included, so a stalled pipeline writing its tenth
    # checkpoint at the identical LSN changes it every single time. That
    # would have verified a stall as a recovery.
    moved = None
    if before.get("position") is not None and after.get("position") is not None:
        moved = after["position"] != before["position"]

    # "seconds since" counts DOWN when something new happens. A smaller
    # number after than before means a write landed in between; None before
    # and a number after means the first write ever.
    target_moved = (
        after["target_seconds_since_write"] is not None
        and (before["target_seconds_since_write"] is None
             or after["target_seconds_since_write"]
             < before["target_seconds_since_write"])
    )
    checkpoint_moved = (
        after["checkpoint_fingerprint"] != before["checkpoint_fingerprint"]
    )

    if not healthy:
        outcome, detail = False, f"still in {after['status']}"
    elif moved is True:
        outcome, detail = True, (f"the read position advanced from "
                                 f"{before['position']} to {after['position']}")
    elif moved is False:
        # The one case the old code got backwards. Healthy, checkpointing,
        # and standing exactly still.
        outcome, detail = False, (
            f"the read position is still {after['position']} — it is "
            f"{after['status']} and checkpointing, but it has not moved")
    elif target_moved or checkpoint_moved:
        signs = []
        if target_moved:
            signs.append("target wrote new data")
        if checkpoint_moved:
            signs.append("checkpoint advanced")
        outcome, detail = True, " and ".join(signs)
    else:
        silence_normal = after["source_behaviour"] in (
            "file_reader", "batch_reader", "api_poller", "spreadsheet_reader")
        outcome, detail = None, (
            f"{after['status']}, but nothing has moved yet"
            + (" - normal for this kind of source once it has read everything"
               if silence_normal else "")
            + ". Cannot confirm it is really working."
        )

    result = {"recovered": outcome, "detail": detail,
              "before": before, "after": after,
              "waited_seconds": wait}
    audit({"app": app, "verification": {"recovered": outcome, "detail": detail}})
    return result
