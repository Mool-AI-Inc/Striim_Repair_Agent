"""
Command line
============

    python -m agent monitor sarah_moolai.Trial5 sarah_moolai.Trial3
    python -m agent watch --namespace sarah_moolai
    python -m agent diagnose sarah_moolai.Trial5      one-shot, no watching
    python -m agent slack                             Socket Mode console
    python -m agent doctor                            can I reach anything?
    python -m agent tools get_status sarah_moolai.Trial5

Every command takes --offline <fixture.json>, which points the whole agent
at a captured health record instead of the network. That is how the
acceptance tests run, and it is a useful way to rehearse the demo on a
train.
"""

import argparse
import json
import sys

from . import config
from . import striim_tools as st
from .nodes import Services
from .registry import Registry
from .slack_io import Console, Notifier
from .striim_actions import Ledger
from .supervisor import Supervisor


def _banner():
    settings = config.summary()
    width = max(len(k) for k in settings)
    print("Striim SRE agent")
    for key, value in settings.items():
        print(f"  {key.replace('_', ' '):<{width}}  {value}")
    if config.DRY_RUN:
        print("  (dry run: every write is printed, nothing is sent. "
              "STRIIM_LIVE=1 to act for real)")
    print()


def _model_client():
    """An Anthropic client, or None with a reason printed.

    None is survivable: the agent still watches, still triages, and still
    escalates - it simply says it could not diagnose. That is a much better
    failure than refusing to start.
    """
    import os
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("  ! ANTHROPIC_API_KEY is not set — the agent will watch and "
              "escalate, but will not diagnose.\n")
        return None
    try:
        import anthropic
        return anthropic.Anthropic()
    except Exception as error:
        print(f"  ! could not create the model client ({error}) — "
              "diagnosis is disabled.\n")
        return None


def build_services(args):
    striim = (st.Striim.offline(args.offline) if args.offline
              else st.Striim())

    if args.offline:
        print(f"  offline: reading {args.offline}\n")

    return Services(
        striim=striim,
        model=None if args.no_model else _model_client(),
        notifier=Notifier(),
        ledger=Ledger(),
        evidence_wait=args.evidence_wait,
        verify_wait=args.verify_wait,
        verbose=not args.quiet,
    )


# ============================================================= COMMANDS

def cmd_monitor(args):
    _banner()
    services = build_services(args)
    registry = Registry()

    if args.apps:
        rejected = [a for a in args.apps if registry.why_not(a)]
        for app in rejected:
            print(f"  ! {registry.why_not(app)}")
        registry.replace([a for a in args.apps if not registry.why_not(a)])

    if not registry.all():
        print("Nothing to watch. Pass pipeline names, or use "
              "`watch --namespace`.")
        return 1

    print(f"Watching {', '.join(registry.all())}\n")

    supervisor = Supervisor(services, registry)
    console = Console(supervisor)
    if console.available:
        console.start_in_background()
        print("  Slack console is live — try `status` in the channel.\n")

    try:
        supervisor.run(interval=args.interval, forever=not args.once)
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


def cmd_watch(args):
    _banner()
    services = build_services(args)
    registry = Registry()

    namespace = args.namespace or config.NAMESPACE
    apps = st.list_apps(services.striim, namespace)["apps"]

    if not apps:
        print(f"No pipelines found in {namespace}.")
        return 1

    if namespace != config.NAMESPACE:
        print(f"  ! {namespace} is not the agent's namespace "
              f"({config.NAMESPACE}) — it can diagnose these but will "
              f"refuse to act on them.\n")

    registry.replace(sorted(apps))
    print(f"Watching all {len(apps)} pipeline(s) in {namespace}:")
    for name, status in sorted(apps.items()):
        print(f"  • {name} — {status}")
    print()

    supervisor = Supervisor(services, registry)
    console = Console(supervisor)
    if console.available:
        console.start_in_background()

    try:
        supervisor.run(interval=args.interval, forever=not args.once)
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


def cmd_diagnose(args):
    _banner()
    services = build_services(args)
    supervisor = Supervisor(services, Registry())
    # Synchronously: this process exits the moment we return, so a
    # background thread would be killed before it ever looked at anything.
    supervisor.diagnose(args.app, background=False)
    return 0


def cmd_slack(args):
    """Slack is the console. Watching continues in the background."""
    _banner()
    services = build_services(args)
    registry = Registry()
    supervisor = Supervisor(services, registry)

    console = Console(supervisor)
    if not console.available:
        print("Socket Mode needs both SLACK_BOT_TOKEN (xoxb-) and "
              "SLACK_APP_TOKEN (xapp-). See setup.example.bat.")
        return 1

    console.start_in_background()
    services.notifier.say(
        f"Striim SRE agent is up — watching {len(registry)} pipeline(s) "
        f"[{'LIVE' if config.LIVE else 'DRY RUN'}]. `help` for commands.")

    try:
        supervisor.run(interval=args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


def cmd_tools(args):
    """Run one tool and print what it returns. For debugging.

    Dispatches through the toolbox, so it reaches BOTH registries. It used
    to know only about the Striim tools, which meant every database tool -
    including `reconcile`, the one that answers whether the target actually
    holds what Striim says it moved - answered "No such tool" from the
    command line while being perfectly available to the model.
    """
    from . import toolbox

    if args.tool not in toolbox.names():
        print(f"No such tool: {args.tool}\n")
        width = max(len(n) for n in toolbox.names())
        for name, description in toolbox.described().items():
            first = (description or "").split(".")[0][:88]
            print(f"  {name:<{width}}  {first}")
        return 1

    striim = (st.Striim.offline(args.offline) if args.offline else st.Striim())
    output = toolbox.call(args.tool, striim, *args.tool_args)

    # db_config is meant to be read by a person deciding what to set, so it
    # gets a laid-out form rather than JSON. Everything else is JSON.
    if args.tool == "db_config":
        from .db import describe_config
        print(describe_config(output))
        return 0

    print(json.dumps(output, indent=2, default=str))
    return 0


def cmd_demo(args):
    """The five beats, offline, from captured records of the real cluster."""
    from .demo import run

    model = None
    if not args.scripted:
        model = _model_client()      # None if there is no key; run() copes
    return run(pause=0 if args.fast else args.pause, model=model)


def cmd_drift(args):
    """Compare a pipeline's schemas now, without waiting for the schedule."""
    from . import drift

    _banner()
    services = build_services(args)
    report = drift.check(services.striim, args.app)

    if report["drifted"]:
        print(drift.compose(report))
    else:
        print(f"\n{args.app}: {report.get('note')}")
    if report.get("ddl", {}).get("note"):
        print(f"\n  DDL: {report['ddl']['note']}")
    return 0


def cmd_capture(args):
    """Save real health records so the tests do not need a live cluster."""
    from .capture import capture, watch_and_capture

    striim = st.Striim()
    if args.once:
        path, status = capture(striim, args.app, label=args.label,
                               directory=args.into)
        print(f"{status} -> {path}")
        return 0

    try:
        watch_and_capture(striim, args.app, directory=args.into,
                          poll=args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


def cmd_doctor(args):
    """Prove the machine can reach everything before the demo, not during."""
    _banner()
    ok = True

    print("Striim")
    try:
        striim = (st.Striim.offline(args.offline) if args.offline
                  else st.Striim())
        apps = st.list_apps(striim, None)
        print(f"  ✓ connected — {apps['count']} pipeline(s) visible")
        for status, count in sorted(apps["by_status"].items()):
            print(f"      {count:>3}  {status}")
        mine = st.list_apps(striim, config.NAMESPACE)
        print(f"  ✓ {mine['count']} in {config.NAMESPACE} (the only ones it "
              f"may act on)")
    except Exception as error:
        ok = False
        print(f"  ✗ {type(error).__name__}: {error}")
        print("    check STRIIM_USER / STRIIM_PASS / STRIIM_URL")

    print("\nModel")
    if _model_client():
        print("  ✓ ANTHROPIC_API_KEY is set")
    else:
        ok = False

    # Three distinct, legitimate states - not two. Running with outbound
    # only is a perfectly reasonable way to demo, so it must not read as a
    # failure, and it must not read as "everything works" either.
    print("\nSlack")
    notifier = Notifier(echo=False)
    outbound = False

    if not notifier.connected:
        print("  – no SLACK_BOT_TOKEN")
    else:
        try:
            identity = notifier.client.auth_test()
            outbound = True
            print(f"  ✓ outbound: posting to {config.SLACK_CHANNEL} "
                  f"as {identity.get('user', 'the bot')}")
        except Exception as error:
            print(f"  ✗ outbound: bot token rejected — {error}")

    if not config.SLACK_APP_TOKEN:
        print("  – no SLACK_APP_TOKEN")
    elif not config.SLACK_APP_TOKEN.startswith("xapp-"):
        print("  ✗ inbound: SLACK_APP_TOKEN should start with 'xapp-'")
    elif outbound:
        print("  ✓ inbound: Socket Mode — monitor / unmonitor / status / "
              "diagnose")
    else:
        print("  ✗ inbound: an app token without a working bot token "
              "cannot reply")

    if outbound and config.SLACK_APP_TOKEN:
        state = "full — incidents, threads, emoji and inbound commands"
    elif outbound:
        state = ("OUTBOUND ONLY — incidents and threads will post, but "
                 "`monitor` from Slack will not work. Use the CLI to "
                 "control it. This is a fine way to run a demo.")
    else:
        state = ("TERMINAL ONLY — every message prints here instead. The "
                 "agent works exactly the same otherwise.")
    print(f"  → {state}")

    print("\n" + ("Ready." if ok else "Not ready — see above."))
    return 0 if ok else 1


# ================================================================ ENTRY

def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m agent",
        description="An SRE for Striim pipelines that knows when NOT to "
                    "restart something.")

    parser.add_argument("--offline", metavar="FIXTURE",
                        help="read a captured health record instead of the "
                             "network")
    parser.add_argument("--no-model", action="store_true",
                        help="skip diagnosis (watch and escalate only)")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--interval", type=int, default=config.POLL_SECONDS,
                        help=f"seconds between polls "
                             f"(default {config.POLL_SECONDS})")
    parser.add_argument("--evidence-wait", type=int,
                        default=config.EVIDENCE_WAIT_SECONDS,
                        help="seconds to wait for error text to appear")
    parser.add_argument("--verify-wait", type=int,
                        default=config.VERIFY_WAIT_SECONDS,
                        help="seconds to wait before verifying a fix — must "
                             "outlast the ~2 minute /health lag")
    parser.add_argument("--once", action="store_true",
                        help="one pass, then exit")

    commands = parser.add_subparsers(dest="command", required=True)

    monitor = commands.add_parser("monitor", help="watch named pipelines")
    monitor.add_argument("apps", nargs="*")
    monitor.set_defaults(run=cmd_monitor)

    watch = commands.add_parser("watch", help="watch a whole namespace")
    watch.add_argument("--namespace", default=config.NAMESPACE)
    watch.set_defaults(run=cmd_watch)

    diagnose = commands.add_parser("diagnose",
                                   help="investigate one pipeline, once")
    diagnose.add_argument("app")
    diagnose.set_defaults(run=cmd_diagnose)

    slack = commands.add_parser("slack",
                                help="run with Slack as the console")
    slack.set_defaults(run=cmd_slack)

    tools = commands.add_parser("tools", help="run one read-only tool")
    tools.add_argument("tool")
    tools.add_argument("tool_args", nargs="*")
    tools.set_defaults(run=cmd_tools)

    demo = commands.add_parser(
        "demo", help="the five beats, offline — the fallback for the day")
    demo.add_argument("--fast", action="store_true",
                      help="no pauses between beats")
    demo.add_argument("--pause", type=float, default=2.0)
    demo.add_argument("--scripted", action="store_true",
                      help="force the deterministic stand-in model")
    demo.set_defaults(run=cmd_demo)

    drift_now = commands.add_parser(
        "drift", help="compare a pipeline's schemas now")
    drift_now.add_argument("app")
    drift_now.set_defaults(run=cmd_drift)

    capture = commands.add_parser("capture",
                                  help="save a health record as a fixture")
    capture.add_argument("app")
    capture.add_argument("--label", help="fixture filename, without .json")
    capture.add_argument("--into", default="tests/fixtures")
    capture.set_defaults(run=cmd_capture)

    doctor = commands.add_parser("doctor", help="check every connection")
    doctor.set_defaults(run=cmd_doctor)

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
