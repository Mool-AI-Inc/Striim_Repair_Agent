"""
The demo, offline
=================

    python -m agent demo

Five beats, run against captured records of the real cluster. No Striim,
no network, no API key required - so if the live cluster misbehaves on the
day, this is the fallback and it tells exactly the same story.

The beats, in the order the argument needs them:

  1  the problem      228 of 576 pipelines in HALT, on Striim's own cluster
  2  silence          a healthy pipeline, and a stopped one - nothing posted
  3  an incident      a real fault, diagnosed down to the actual file
  4  a refusal        it will NOT restart, and says exactly why
  5  a repair         a fault it can fix: restart, verify, close

Then the contrast, which is the whole argument in one screen: three
pipelines, the same eight tools, three different answers.

If ANTHROPIC_API_KEY is set the real model does the diagnosing. If it is
not, a scripted stand-in supplies the verdicts so the run is identical
every time - and the banner says which one is talking. Either way every
GUARDRAIL is the real one: the refusals below are produced by the same
code that runs in production, not by the script.
"""

import json
import time
from pathlib import Path

from . import config
from . import nodes
from . import skills as skill_layer
from . import slack_io
from . import striim_actions as act
from . import striim_tools as st
from .graph import build_diagnose_graph, build_graph
from .state import new_incident

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"

TRIAL5 = "sarah_moolai.Trial5"
TRIAL3 = "sarah_moolai.Trial3"
ORACLE = "MANIMARAN22.OracleToFabricDWH_CDC"
SALESFORCE = "jobregonr.salesforce2bigquery"

RULE = "─" * 74


# ============================================================== SCRIPTED

# What the stand-in model says. Note beat 4: it is deliberately WRONG, and
# confidently so. The code overrules it. That is the point of the beat, and
# it is why this is scripted rather than left to chance.
SCRIPT = {
    TRIAL5: {
        "healthy": False,
        "headline": "Source file went missing",
        "what_happened": "The file reader lost its source part way through.",
        "likely_cause": "AI.csv is no longer in the upload directory",
        "fault_zone": "pipeline",
        "confidence": "high",
        "evidence": "reader idle, position 171,000 into a file that is gone",
        "restart_would_help": True,
        "why_not": "",
        "human_action_needed": "",
        "suggested_skill": "wait_and_resume",
        "suggested_tier": 1,
        "missing_information": "",
    },
    SALESFORCE: {
        "healthy": False,
        "headline": "Poller stopped after an API error",
        "what_happened": "The Salesforce reader halted and stayed down.",
        "likely_cause": "a transient API failure at the source",
        "fault_zone": "pipeline",
        "confidence": "high",
        "evidence": "recorded position intact, nothing refused by the target",
        "restart_would_help": True,
        "why_not": "",
        "human_action_needed": "",
        "suggested_skill": "wait_and_resume",
        "suggested_tier": 1,
        "missing_information": "",
    },
    ORACLE: {
        "healthy": False,
        "headline": "Oracle capture has no resume position",
        "what_happened": "Change capture stopped and never wrote a checkpoint.",
        "likely_cause": "recovery is on but no checkpoint was ever recorded",
        "fault_zone": "pipeline",
        "confidence": "high",
        "evidence": "every recorded checkpoint is N/A",
        "restart_would_help": True,
        "why_not": "",
        "human_action_needed": "",
        "suggested_skill": "wait_and_resume",
        "suggested_tier": 1,
        "missing_information": "",
    },
    TRIAL3: {
        "healthy": False,
        "headline": "Stopped with no recovery configured",
        "what_happened": "The pipeline is off and has no saved position.",
        "likely_cause": "recovery was never enabled on this application",
        "fault_zone": "pipeline",
        "confidence": "high",
        "evidence": "no checkpoint data at all",
        "restart_would_help": True,
        "why_not": "",
        "human_action_needed": "",
        "suggested_skill": "wait_and_resume",
        "suggested_tier": 1,
        "missing_information": "",
    },
}


class ScriptedModel:
    """Stands in for the model so the fallback run is identical every time."""

    def __init__(self, script):
        self.script = script
        self.messages = self

    def create(self, **kwargs):
        opening = kwargs["messages"][0]["content"]
        verdict = next((v for app, v in self.script.items() if app in opening),
                       None) or {
            "healthy": True, "headline": "Nothing wrong",
            "what_happened": "", "likely_cause": "none",
            "fault_zone": "pipeline", "confidence": "high", "evidence": "",
            "restart_would_help": False, "why_not": "",
            "human_action_needed": "", "suggested_skill": None,
            "suggested_tier": 3, "missing_information": "",
        }

        class Block:
            type = "text"
            text = json.dumps(verdict)

        class Reply:
            content = [Block()]
            stop_reason = "end_turn"

        return Reply()


# ============================================================== PLUMBING

class Screen(slack_io.Notifier):
    """Prints what would go to Slack, indented like a thread."""

    def __init__(self):
        super().__init__(token=None, channel="#striim-incidents", echo=False)
        self.posts = []

    def post_incident(self, text):
        self.posts.append(text)
        print()
        print(_shift(text, "    "))
        return super().post_incident(text)

    def reply(self, handle, text, kind=None):
        print(_shift(text, "      └ "))
        return super().reply(handle, text, kind=kind)

    def set_state(self, handle, emoji):
        print(f"      └ parent reaction is now {emoji}")
        return super().set_state(handle, emoji)


def _shift(text, prefix):
    lines = text.splitlines()
    pad = " " * len(prefix)
    return "\n".join((prefix if i == 0 else pad) + line
                     for i, line in enumerate(lines))


def _services(fixture, model, notifier):
    return nodes.Services(
        striim=st.Striim.offline(FIXTURES / fixture),
        model=model,
        notifier=notifier,
        ledger=act.Ledger(path=config.STATE_DIR / "demo-ledger.json"),
        evidence_wait=0, evidence_attempts=0, verify_wait=0, verbose=False,
    )


def _beat(number, title, says):
    print(f"\n{RULE}\n  BEAT {number}   {title}\n{RULE}")
    for line in says:
        print(f"  {line}")


def _pause(seconds):
    if seconds:
        time.sleep(seconds)


# ================================================================= BEATS

def beat_1_the_problem(pause):
    _beat(1, "The problem — their cluster, not our claim", [
        "This is Striim's own shared development cluster, as captured.",
    ])
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    everything = st.list_apps(striim, None)

    print(f"\n    {everything['count']} pipelines visible\n")
    for status, count in sorted(everything["by_status"].items(),
                                key=lambda kv: -kv[1]):
        bar = "█" * max(1, round(count / 6))
        print(f"      {count:>4}  {status:<12} {bar}")

    broken = (everything["by_status"].get("HALT", 0)
              + everything["by_status"].get("TERMINATED", 0))
    print(f"\n    {broken} of {everything['count']} are broken right now.")
    print("    Every one of them alerted when it broke. Not one of the")
    print("    alerts said WHY, and that is the twenty minutes at 3am.")
    _pause(pause)


def beat_2_silence(pause, model):
    _beat(2, "Silence when healthy", [
        "The agent is watching. A working pipeline says nothing at all,",
        "and neither does one somebody switched off on purpose.",
    ])

    for fixture, label in (("trial5_running.json", "Trial5, running"),
                           ("staged_deliberate_stop.json",
                            "Trial3, stopped by a person")):
        screen = Screen()
        services = _services(fixture, model, screen)
        app = json.loads((FIXTURES / fixture).read_text())["captured_from"]
        final = build_graph(services).invoke(new_incident(app))
        posted = "posted an incident" if screen.posts else "said nothing"
        print(f"\n    {label:<32} → {posted}   ({final.get('reason')})")

    print("\n    No 'all clear' every minute. A channel that chirps when")
    print("    nothing is wrong is a channel people mute.")
    _pause(pause)


def beat_3_an_incident(pause, model):
    _beat(3, "A real incident, diagnosed", [
        "Trial5 has broken. Watch what the incident actually names.",
    ])

    screen = Screen()
    services = _services("trial5_halt.json", model, screen)
    final = build_graph(services).invoke(new_incident(TRIAL5))

    config_seen = final["evidence"]["source_config"]
    print(f"\n    It did that with NO error text — this cluster publishes")
    print("    none, for any of its 576 pipelines. The file name came from")
    print("    the component's own configuration:")
    print(f"\n        adapter  {config_seen['adapter']}")
    print(f"        reads    {config_seen['expects']}")
    position = config_seen["read_position"]
    print(f"        got to   byte {position['offset_end_bytes']:,} "
          f"of {position['source_name']}")
    print("\n    Not 'check the source system'. A path someone can act on.")
    _pause(pause)
    return final


def beat_4_the_refusal(pause, final):
    _beat(4, "The refusal — the part that is actually the product", [
        "Anyone can restart a pipeline. Striim ships that as a flag.",
    ])

    print(f"\n    The diagnosis said:  restart it, HIGH confidence")
    print(f"    The agent did:       nothing")
    print(f"\n    Because:  {final['reason']}")
    print("\n    That refusal is not the model being careful. The model")
    print("    asked to restart and was overruled in Python — the recorded")
    print("    position is 171,000 bytes into a file that is gone, and")
    print("    nothing available proves otherwise. A restart re-reads from")
    print("    the start into an append-only target.")
    print("\n    No alert would ever fire for that. The pipeline would look")
    print("    healthy. The duplicate rows would be found by whoever next")
    print("    queried a total, weeks later.")
    _pause(pause)


def beat_5_the_repair(pause, model):
    _beat(5, "A repair it CAN make", [
        "A different pipeline, a fault whose cause has cleared.",
        "Salesforce to BigQuery — a connector it has no specific code for.",
    ])

    original = config.NAMESPACE
    config.NAMESPACE = "jobregonr"
    try:
        screen = Screen()
        services = _services("staged_safe_restart.json", model, screen)
        final = build_graph(services).invoke(new_incident(SALESFORCE))
    finally:
        config.NAMESPACE = original

    verification = final.get("verification") or {}
    print(f"\n    Verified: {verification.get('detail')}")
    print("\n    Not 'it says RUNNING'. It waited out the monitoring lag,")
    print("    then checked the target had written since the restart and")
    print("    the checkpoint had moved. Three outcomes, not two — the")
    print("    third is 'I cannot confirm this', and it says that too.")
    _pause(pause)
    return final


def beat_6_repairs(pause, model):
    _beat(6, "Repairs beyond a restart", [
        "A restart is one repair. These are the others it knows about,",
        "and each one checks its own preconditions in code.",
    ])

    print()
    for skill in skill_layer.SKILLS.values():
        acts = {1: "can act alone", 2: "proposes only",
                3: "never acts"}[skill.max_tier]
        undo = "" if skill.reversible else "  · cannot be undone"
        print(f"    {skill.name:<32} {skill.zone:<9} {acts}{undo}")

    print("\n    Only two can act unattended, and the second one only under")
    print("    a measured row count. The cap is in each skill's own class,")
    print("    so no amount of confidence anywhere can raise it.")

    print("\n    The model may suggest one. It does not get to run one:")
    print("    the code re-checks the evidence itself and declines if it")
    print("    is not there — and says so, because the refusal is often")
    print("    the more useful sentence.\n")

    # The refusal that is itself a finding. This is the other half of the
    # central argument: we showed a re-read duplicates silently, and this
    # is why that damage cannot simply be repaired afterwards.
    services = _services("trial5_halt.json", model, Screen())
    state = nodes.watch({"app": TRIAL5}, services)
    state["app"] = TRIAL5
    state["evidence"] = nodes.evidence(state, services)["evidence"]
    state["reread_confirmed"] = True
    state["verdict"] = {"fault_zone": "target"}

    _, _, dropped = skill_layer.choose(state, "dedupe_target_rows")
    print(f"    Asked to deduplicate Trial5's target:\n")
    print(f"      {dropped}")
    print("\n    That sentence is the finding. A duplicated file cannot be")
    print("    repaired afterwards — which is exactly why the restart was")
    print("    refused rather than allowed and cleaned up later.")
    _pause(pause)


def the_contrast(pause, model):
    print(f"\n{RULE}\n  THE CONTRAST   same eight tools, three answers\n{RULE}")

    rows = [
        ("trial5_halt.json", TRIAL5, "file reader, position recorded"),
        ("real_trial3_stopped.json", TRIAL3, "file reader, no recovery"),
        ("real_oracle_cdc_terminated.json", ORACLE, "Oracle CDC, checkpoints N/A"),
    ]

    print()
    for fixture, app, description in rows:
        services = _services(fixture, model, Screen())
        state = nodes.watch({"app": app}, services)
        state["app"] = app
        evidence = nodes.evidence({**state, "app": app}, services)["evidence"]
        risk = nodes.restart_risk(evidence)
        behaviour = (evidence["flow"]["source"]["profile"] or {}).get("behaviour")

        print(f"    {app}")
        print(f"        {description}   ({behaviour})")
        print(f"        → {risk or 'a restart is safe'}")
        print()

    print("    Three different answers. One of them is somebody else's")
    print("    Oracle pipeline, in a namespace this agent may read and may")
    print("    never touch, on a connector nobody wrote code for.")
    print("\n    The tool layer does not grow when a connector is added.")
    print("    Only the knowledge does.")
    _pause(pause)


# ================================================================== RUN

def run(pause=2.0, model=None):
    scripted = model is None
    if scripted:
        model = ScriptedModel(SCRIPT)

    config.ensure_state_dir()
    act.Ledger(path=config.STATE_DIR / "demo-ledger.json").clear()

    print(f"\n{RULE}")
    print("  STRIIM PIPELINE SRE AGENT — offline demo")
    print(f"{RULE}")
    print("  Captured records of the real shared cluster. No network.")
    print(f"  Diagnosis by: {'a scripted stand-in (deterministic)' if scripted else 'the live model'}")
    print("  Guardrails:   the real ones — every refusal below is produced")
    print("                by the same code that runs in production.")

    beat_1_the_problem(pause)
    beat_2_silence(pause, model)
    incident = beat_3_an_incident(pause, model)
    beat_4_the_refusal(pause, incident)
    beat_5_the_repair(pause, model)
    beat_6_repairs(pause, model)
    the_contrast(pause, model)

    print(f"\n{RULE}")
    print("  Striim tells you what it did. Nothing tells you what is")
    print("  actually in the target. This closes that gap — and knows")
    print("  when not to.")
    print(f"{RULE}\n")
    return 0
