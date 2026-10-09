"""
The loop around the graph
=========================

The graph handles one incident. This handles "keep watching these, forever,
and let people change the list while you do".

It is also the object Slack talks to: add / remove / status / diagnose are
exactly the four inbound commands, so the Slack layer needs no knowledge of
anything below it.
"""

import threading
import time
import traceback

from . import config
from . import ddl
from . import drift
from . import slack_io
from . import striim_actions as act
from . import striim_tools as st
from .graph import build_diagnose_graph, build_graph
from .state import new_incident, short_name


class Supervisor:
    def __init__(self, services, registry):
        self.services = services
        self.registry = registry
        self.graph = build_graph(services)
        self.one_shot = build_diagnose_graph(services)
        self.last_seen = {}

        # Schema drift is checked on its own schedule, not every poll, and
        # each distinct drift is mentioned once rather than every time it
        # is still true.
        self.drift_checked = {}
        self.drift_warned = set()

        # Investigations run on one worker so the status sweep never waits
        # behind a pipeline that is taking four minutes to verify.
        self._queued = []
        self._working_on = None
        self._worker = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------- loop

    def tick(self):
        """One pass: read every status, then queue whatever needs looking at.

        THE SWEEP IS NEVER BLOCKED BY AN INVESTIGATION. It used to be -
        the loop called the whole graph for each app in turn, and a
        faulted pipeline takes minutes (a model call, an evidence wait, a
        210-second verification). Everything behind it in the list sat at
        "not checked yet" the whole time, which is the least useful thing
        `status` can say about a pipeline that is running perfectly well.

        So: one cheap read gives every status, and the expensive part
        happens on a worker.
        """
        # Anything nobody answered gets abandoned here, on the loop that
        # is already running - no timer thread, and it cannot outlive the
        # process that would report it.
        self.services.expire_approvals()

        statuses = self.sweep()
        for app in self.registry.all():
            self.investigate_later(app)
        return statuses

    def sweep(self):
        """Every monitored pipeline's status, from ONE health call.

        /health carries the whole cluster, so this costs the same whether
        one pipeline is watched or forty.
        """
        try:
            everything = st.list_apps(self.services.striim, None)["apps"]
        except Exception as error:
            print(f"    could not read the cluster: "
                  f"{type(error).__name__}: {error}")
            return {}

        seen = {}
        for app in self.registry.all():
            status = everything.get(app, "NOT FOUND")
            self.last_seen[app] = status
            seen[app] = status
        return seen

    def investigate_later(self, app):
        """Queue an app for the worker, unless it is already in hand."""
        with self._lock:
            if app in self._queued or app == self._working_on:
                return False
            self._queued.append(app)
        self._ensure_worker()
        return True

    def _ensure_worker(self):
        """One worker, not one per pipeline.

        Investigations share a Striim client, a ledger on disk and the
        open-incident book. Running several at once would need all three
        made concurrent for no benefit anybody asked for - what was wanted
        is that the sweep does not wait, and a single worker delivers that.
        """
        with self._lock:
            if self._worker and self._worker.is_alive():
                return
            self._worker = threading.Thread(target=self._drain, daemon=True,
                                            name="investigator")
            self._worker.start()

    def _drain(self):
        while True:
            with self._lock:
                if not self._queued:
                    self._working_on = None
                    return
                app = self._queued.pop(0)
                self._working_on = app

            try:
                self.check_drift(app)
                final = self.graph.invoke(new_incident(app))
                if final.get("status"):
                    self.last_seen[app] = final["status"]
            except Exception as error:
                # One broken pipeline must not stop the agent watching the
                # others. Print the traceback and carry on round the loop.
                print(f"    {short_name(app)}: {type(error).__name__}: {error}")
                traceback.print_exc()
            finally:
                with self._lock:
                    self._working_on = None

    def drain_now(self):
        """Run the queue on this thread. For the CLI and the tests."""
        self._drain()

    def check_drift(self, app, force=False):
        """Compare the schemas on a schedule, and warn - never raise.

        Runs for every monitored pipeline whether or not anything has
        broken, because by the time a fault brings the agent here the
        target has already been behind for hours.

        What comes out is a WARNING, deliberately: nothing is broken, and
        treating it like an outage would say "production is down" about a
        pipeline running perfectly well. Said once per distinct drift.
        """
        due = time.time() - self.drift_checked.get(app, 0)
        if not force and due < config.SCHEMA_CHECK_SECONDS:
            return None

        self.drift_checked[app] = time.time()
        try:
            report = drift.check(self.services.striim, app)
        except Exception as error:
            print(f"    drift check failed for {short_name(app)}: "
                  f"{type(error).__name__}: {error}")
            return None

        if not report["drifted"]:
            return report

        mark = drift.fingerprint(report)
        if mark in self.drift_warned:
            return report              # already said, once is enough

        self.drift_warned.add(mark)
        act.audit({"drift": "warned", "app": app,
                   "findings": report["findings"]})
        self.services.notifier.say(drift.compose(report))
        return report

    def run(self, interval=None, forever=True):
        interval = config.POLL_SECONDS if interval is None else interval
        watching = self.registry.all()

        if not watching:
            print("Nothing to watch yet. Add something with "
                  "`monitor ns.AppName` in Slack, or pass names on the "
                  "command line.")

        while True:
            self.tick()
            if not forever:
                return
            time.sleep(interval)

    # -------------------------------------- the five inbound commands
    #
    # Every one of these returns a sentence IMMEDIATELY. A first check runs
    # a model call and a verification wait behind it, which can take a
    # minute or more - and a console that goes silent for a minute reads as
    # broken, especially with an audience watching. So the reply is the
    # acknowledgement and the finding arrives afterwards as an incident.

    def _in_background(self, name, work):
        thread = threading.Thread(target=work, daemon=True, name=name)
        thread.start()
        return thread

    def add(self, app):
        ok, message = self.registry.add(app)
        if not ok:
            return message

        self.sweep()
        self.investigate_later(app)
        return f"{message} Checking it now — I'll post here if anything is wrong."

    def remove(self, app):
        _, message = self.registry.remove(app)
        self.services.open_incidents.pop(app, None)
        return message

    # ------------------------------------------ approving a destructive fix

    def approve(self, thread):
        """Somebody said yes. Capture, run, resume, verify."""
        pending = self.services.pending.get(thread)
        if not pending:
            return None

        if self.services.notifier.is_closed(thread):
            return ("This incident is already marked resolved, so I have "
                    "left it alone.")

        if pending["state"] != "waiting":
            # Only one pending approval per incident, and a second yes is
            # not a second run.
            return (f"Nothing to approve — this one was already "
                    f"{pending['state']}.")

        pending["state"] = "approved"
        act.audit({"approval": "granted", "app": pending["app"],
                   "incident": pending["fingerprint"],
                   "statement": pending["summary"],
                   "backup": pending["backup"]})

        self._in_background(f"approved-{pending['app']}",
                            lambda: self._carry_out(pending))
        return f"Running it now — I'll post the result here."

    def skip(self, thread):
        pending = self.services.pending.get(thread)
        if not pending:
            return None
        if pending["state"] != "waiting":
            return f"Nothing to skip — this one was already {pending['state']}."

        pending["state"] = "skipped"
        act.audit({"approval": "declined", "app": pending["app"],
                   "incident": pending["fingerprint"],
                   "statement": pending["summary"]})
        return ("Left alone. Nothing was changed and no backup was taken — "
                "the statement is above if you want it later.")

    def _carry_out(self, pending):
        """The continuation. Everything it needs came out of the dict."""
        thread, app = pending["thread"], pending["app"]
        notifier = self.services.notifier

        try:
            ddl.execute(pending["plan"], services=self.services, thread=thread)
        except Exception as error:
            pending["state"] = "failed"
            notifier.reply(thread, f"⛔ it did not run: {error}. Nothing was "
                                   f"changed beyond the backup, if one was "
                                   f"taken.", kind="failure")
            return

        before = act.snapshot(self.services.striim, app)
        try:
            result = act.run_command(self.services.striim, app, "RESUME",
                                     fingerprint=pending["fingerprint"],
                                     ledger=self.services.ledger)
            notifier.reply(thread, f"Sent `{result['statement']}`.",
                           kind="action")
        except act.ActionRefused as error:
            notifier.reply(thread, f"The repair ran, but the resume was "
                                   f"refused: {error}", kind="failure")
            return

        outcome = act.verify(self.services.striim, app, before,
                             wait=self.services.verify_wait,
                             verbose=self.services.verbose)
        notifier.reply(thread, slack_io.compose_verification(app, outcome),
                       kind="outcome")
        if outcome["recovered"] is True:
            notifier.set_state(thread, slack_io.RESOLVED)
            self.services.open_incidents.pop(app, None)
        else:
            notifier.set_state(thread, slack_io.NEEDS_HUMAN)

    def human_closed(self, timestamp):
        """Somebody put ✅ on an incident. Stand down on that one.

        We do not re-raise it, do not remove their reaction, and do not
        post into that thread again. If they were wrong, the next DIFFERENT
        fault will still raise a new incident - but arguing with a person
        who has just said "I've got this" is how an agent gets muted.
        """
        handle = self.services.notifier.close_by_human(timestamp)
        if not handle:
            return None
        for app, incident in self.services.open_incidents.items():
            if incident.get("thread") == handle:
                incident["closed_by_human"] = True
                return app
        return None

    def status(self):
        watching = self.registry.all()
        if not watching:
            return ("Not watching anything. `monitor "
                    f"{config.NAMESPACE}.YourApp` to start.")

        # Ask the cluster now rather than reporting whatever the loop last
        # happened to reach. `status` is a question, and answering it with
        # "not checked yet" for a pipeline that has been running fine all
        # morning is the wrong answer.
        self.sweep()

        lines = [f"Watching {len(watching)} pipeline(s) "
                 f"[{'LIVE' if config.LIVE else 'DRY RUN'}]:"]
        for app in watching:
            state = self.last_seen.get(app, "not checked yet")
            if app == self._working_on:
                state += " — investigating now"
            elif app in self._queued:
                state += " — queued for investigation"
            open_incident = self.services.open_incidents.get(app)
            if open_incident and open_incident.get("closed_by_human"):
                flag = "  ✅ closed by a human"
            elif open_incident:
                flag = "  🔴 open incident"
            else:
                flag = ""
            lines.append(f"  • `{app}` — {state}{flag}")
        return "\n".join(lines)

    def diagnose(self, app, background=True):
        """A one-off investigation. Reads anywhere; acts only at home.

        `background` is the difference between the two callers. Slack needs
        an answer inside a second and gets the finding afterwards; the CLI
        is a one-shot process that would exit before a background thread
        ever ran, so it waits.
        """

        def investigate():
            try:
                final = self.one_shot.invoke(new_incident(app))
            except Exception as error:
                message = (f"Could not investigate `{app}`: "
                           f"{type(error).__name__}: {error}")
                self.services.notifier.say(message)
                return message

            if final.get("tier") is not None:
                return f"Raised an incident for `{app}` — see the thread."

            # Nothing was raised, so answer the person who asked rather
            # than leaving them with silence.
            verdict = final.get("verdict") or {}
            message = (f"`{app}` — {final.get('status')}. "
                       f"{verdict.get('headline') or 'Nothing wrong.'} "
                       f"{verdict.get('what_happened') or ''}").strip()
            self.services.notifier.say(message)
            return message

        if not background:
            return investigate()

        self._in_background(f"diagnose-{app}", investigate)

        outside = "" if self.registry.in_namespace(app) else (
            f" It's outside `{config.NAMESPACE}`, so I'll read it in full "
            f"and refuse to act on it.")
        return f"Looking at `{app}` now.{outside}"
