"""
Acceptance tests
================

The ten scenarios from PLAN.md section 8, plus the contract and unit checks
underneath them. Everything runs offline in about a second, with no cluster
and no API key.

WHAT THE FIXTURES ARE. All of them come from real captures of the shared
Striim dev cluster - 576 pipelines, of which 228 were in HALT:

  trial5_halt / trial5_running       untouched full-cluster captures
  real_*.json                        one app carved out, data untouched
  staged_*.json                      real records with a SECOND instant
                                     staged on top, because a capture is
                                     one moment and "restarted, then
                                     recovered" needs two. Each says in its
                                     note exactly what was staged.

That distinction matters. The first suite written for this agent passed
against invented fixtures and was wrong about SIX field names, including
two the whole safety argument rests on. See tests_the_contract below - that
is the class of test that would have caught them.

    python tests/test_acceptance.py          plain, no dependencies
    pytest tests/test_acceptance.py -v       if you have pytest

THE TESTS THAT MATTER MOST are 4, 7, 8 and 9. Anyone can build something
that restarts pipelines; Striim ships that as a config flag. Knowing when
restarting is the wrong move is the product, so those four hold the model
to a WRONG opinion on purpose and check the code overrules it anyway.
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent import config                                     # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"

# Point every piece of persisted state at a scratch directory BEFORE
# anything imports it, so a test run never touches the real ledger.
_SCRATCH = Path(tempfile.mkdtemp(prefix="striim-agent-tests-"))
config.STATE_DIR = _SCRATCH
config.REGISTRY_FILE = _SCRATCH / "registry.json"
config.LEDGER_FILE = _SCRATCH / "ledger.json"
config.AUDIT_FILE = _SCRATCH / "audit.log"
config.NAMESPACE = "sarah_moolai"
config.DRY_RUN = True
config.LIVE = False

from agent import backfill, db, ddl, drift, nodes, probes     # noqa: E402
from agent import profiles                                    # noqa: E402
from agent import prompts, slack_io                            # noqa: E402
from agent import skills, target_repair                        # noqa: E402
from agent import striim_actions as act                       # noqa: E402
from agent import striim_tools as st                          # noqa: E402
from agent.graph import build_diagnose_graph, build_graph      # noqa: E402
from agent.registry import Registry                            # noqa: E402
from agent.state import new_incident                           # noqa: E402
from agent.supervisor import Supervisor                        # noqa: E402

# The real apps these scenarios are built from.
TRIAL5 = "sarah_moolai.Trial5"
TRIAL3 = "sarah_moolai.Trial3"
ORACLE = "MANIMARAN22.OracleToFabricDWH_CDC"
SENTIFILE = "RPFinalProd.SentiFile"
SALESFORCE = "jobregonr.salesforce2bigquery"
REJECTING = "sarah_moolai.Traill"


# ============================================================= HARNESS

class Block:
    """One content block of a model response."""

    def __init__(self, type, text=None, name=None, input=None, id="tu_1"):
        self.type = type
        self.text = text
        self.name = name
        self.input = input or {}
        self.id = id


class Response:
    def __init__(self, content, stop_reason="end_turn"):
        self.content = content
        self.stop_reason = stop_reason


class FakeModel:
    """A model that says exactly what the test tells it to.

    The point of these tests is the code AROUND the model, so the model is
    a fixture like everything else. Several tests deliberately hand it an
    answer that is confidently wrong.
    """

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = 0
        self.messages = self
        self.last_kwargs = None

    def create(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        reply = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        if isinstance(reply, dict):
            return Response([Block("text", text=json.dumps(reply))])
        return reply


class Recorder(slack_io.Notifier):
    """A Notifier that keeps everything instead of sending it."""

    def __init__(self):
        super().__init__(token=None, channel="#test", echo=False)
        self.posts = []
        self.replies = []       # what REACHED the thread, after filtering
        self.withheld = []      # what was offered and kept off Slack
        self.states = []
        self.rewrites = []

    def post_incident(self, text):
        self.posts.append(text)
        return super().post_incident(text)

    def rewrite(self, handle, text):
        self.rewrites.append(text)
        return super().rewrite(handle, text)

    @property
    def final(self):
        """What the parent message says once everything has settled."""
        return self.rewrites[-1] if self.rewrites else (
            self.posts[-1] if self.posts else "")

    def reply(self, handle, text, kind=None):
        """Record what SLACK would see, not what the code offered it.

        The rule under test is that most replies never leave the terminal,
        so a recorder that keeps everything would make every thread look
        full and every assertion about noise pass by accident.
        """
        before = len(self._threads.get(handle, {}).get("sent", []))
        super().reply(handle, text, kind=kind)
        after = self._threads.get(handle, {}).get("sent", [])
        if len(after) > before:
            self.replies.append(text)
        else:
            self.withheld.append(text)

    def set_state(self, handle, emoji):
        self.states.append(emoji)
        return super().set_state(handle, emoji)

    @property
    def everything(self):
        return "\n".join(self.posts + self.replies + self.rewrites)


def verdict(**overrides):
    """A well-formed verdict. Tests override the fields they care about."""
    base = {
        "healthy": False,
        "headline": "Source file went missing",
        "what_happened": "The file reader lost its source part way through.",
        "likely_cause": "AI.csv was deleted from the upload directory",
        "fault_zone": "source",
        "confidence": "high",
        "evidence": "source unreadable, position invalid",
        "restart_would_help": False,
        "why_not": "it will re-read the whole file and duplicate every row",
        "human_action_needed": "restore AI.csv to UploadedFiles/sarah_moolai/",
        "suggested_tier": 3,
        "missing_information": "",
    }
    base.update(overrides)
    return base


def services_for(fixture, model=None, notifier=None, ledger=None,
                 verify_wait=0):
    striim = st.Striim.offline(FIXTURES / fixture)
    return nodes.Services(
        striim=striim,
        model=model if model is not None else FakeModel(verdict()),
        notifier=notifier if notifier is not None else Recorder(),
        ledger=ledger if ledger is not None else act.Ledger(
            path=_SCRATCH / f"ledger-{fixture}"),
        evidence_wait=0,
        evidence_attempts=0,
        verify_wait=verify_wait,
        verbose=False,
    )


def run(fixture, model=None, notifier=None, ledger=None, app=None,
        one_shot=False, verify_wait=0, namespace=None):
    """Run the whole graph against a fixture.

    `namespace` retargets the agent's own namespace for scenarios whose
    only real example lives somewhere else on the cluster.
    """
    services = services_for(fixture, model, notifier, ledger, verify_wait)
    graph = build_diagnose_graph(services) if one_shot else build_graph(services)
    app = app or json.loads((FIXTURES / fixture).read_text())["captured_from"]

    original = config.NAMESPACE
    if namespace:
        config.NAMESPACE = namespace
    try:
        final = graph.invoke(new_incident(app))
    finally:
        config.NAMESPACE = original
    return final, services.notifier, services


def evidence_for(fixture, app=None):
    """The baseline EVIDENCE gathers, without running the whole graph."""
    services = services_for(fixture)
    app = app or json.loads((FIXTURES / fixture).read_text())["captured_from"]
    state = nodes.watch({"app": app}, services)
    state["app"] = app
    return nodes.evidence(state, services)["evidence"]


# =========================================================== SCENARIO 1

def test_healthy_pipeline_says_nothing():
    """A running pipeline produces nothing in Slack. Ever."""
    final, notifier, _ = run("trial5_running.json")
    assert final.get("silent") is True, final.get("reason")
    assert notifier.posts == [], notifier.posts
    assert final.get("tier") is None


def test_a_completed_pipeline_is_not_a_fault():
    """COMPLETED means it finished. Zero throughput there means 'done'."""
    final, notifier, _ = run("real_completed.json")
    assert notifier.posts == [], notifier.posts
    assert final.get("silent") is True
    assert "not a fault" in st.STATUS_MEANING["COMPLETED"]


# =========================================================== SCENARIO 2

def test_deliberate_stop_says_nothing():
    """Someone switched it off. That is not an incident."""
    final, notifier, _ = run("staged_deliberate_stop.json")
    assert notifier.posts == [], notifier.posts
    assert final.get("silent") is True

    striim = st.Striim.offline(FIXTURES / "staged_deliberate_stop.json")
    history = st.get_state_history(striim, TRIAL3)
    assert history["looks_deliberate"] is True, history
    assert "STOPPING" in history["note"]


def test_an_empty_history_is_not_evidence_of_anything():
    """The real Trial3 record has no transitions at all. That is NO DATA."""
    striim = st.Striim.offline(FIXTURES / "real_trial3_stopped.json")
    history = st.get_state_history(striim, TRIAL3)
    assert history["looks_deliberate"] is None, history
    assert "NO DATA" in history["note"]


# =========================================================== SCENARIO 3

def test_real_incident_names_the_actual_file():
    """An incident that says 'check the source system' is worthless.

    Against the real capture there is no error text at all - issuesList was
    empty for all 576 apps - so the file name has to come from the
    component configuration. That is exactly why describe_component exists.
    """
    final, notifier, _ = run("trial5_halt.json")

    assert len(notifier.posts) == 1, notifier.posts
    assert final["tier"] == 3

    seen = final["evidence"]["source_config"]
    assert seen["expects"] == "UploadedFiles/sarah_moolai/AI.csv", seen
    assert seen["adapter"] == "FileReader", seen
    assert "AI.csv" in notifier.everything

    # And it got there with no error text whatsoever.
    assert final["evidence"]["errors"]["error_count"] == 0


# =========================================================== SCENARIO 4

def test_refuses_restart_that_would_duplicate():
    """THE important one, and now against real data.

    The model is handed a confidently wrong answer - restart it, high
    confidence, only a pipeline problem - and the code refuses anyway. On
    the real Trial5 the recorded position is byte 171,000 of AI.csv and
    nothing available confirms that still matches the file that is there.
    """
    wrong = verdict(fault_zone="pipeline", restart_would_help=True,
                    confidence="high", why_not="",
                    likely_cause="a transient read failure")

    final, notifier, services = run("trial5_halt.json", model=FakeModel(wrong))

    assert final["tier"] == 3, (final["tier"], final["reason"])
    assert "duplicates every row" in final["reason"], final["reason"]
    assert "171,000" in final["reason"], final["reason"]
    assert final.get("action_taken") is None
    assert services.ledger.budget_left(TRIAL5) == config.MAX_RESTARTS_PER_HOUR

    # The refusal has to be ON the message, not buried in a reply. At 3am
    # nobody opens the thread to find out why it did not fix itself.
    incident = notifier.posts[0]
    assert "Source file went missing" in incident.splitlines()[0], incident
    assert "Not restarting:" in incident, incident
    assert "duplicates every row" in incident, incident
    assert "You need to:" in incident, incident
    assert "UploadedFiles/sarah_moolai/" in incident, incident


def test_an_overruled_diagnosis_still_gets_a_next_action():
    """When a guardrail wins, the model's 'do this' line is usually empty.

    It was busy recommending a restart. The incident still has to end with
    something a person can do, built from the component configuration.
    """
    wrong = verdict(fault_zone="pipeline", restart_would_help=True,
                    confidence="high", why_not="", human_action_needed="")
    _, notifier, _ = run("trial5_halt.json", model=FakeModel(wrong))

    incident = notifier.posts[0]
    assert "You need to:" in incident, incident
    assert "UploadedFiles/sarah_moolai/AI.csv" in incident, incident


def test_refuses_restart_when_recovery_was_never_enabled():
    """A real file pipeline in HALT with no checkpointInformation at all."""
    wrong = verdict(fault_zone="pipeline", restart_would_help=True,
                    confidence="high", why_not="")
    final, _, _ = run("real_file_no_recovery_halt.json", model=FakeModel(wrong),
                      namespace="RPFinalProd")

    assert final["tier"] == 3, final["reason"]
    assert "nothing is saved to resume from" in final["reason"], final["reason"]


def test_a_cdc_source_with_no_position_would_skip_not_duplicate():
    """The other half of the rule. Change capture cannot re-read - it misses.

    Real MANIMARAN22 Oracle pipeline: recovery is switched on but every
    checkpoint is N/A, so a restart resumes from now and loses the gap.
    """
    wrong = verdict(fault_zone="pipeline", restart_would_help=True,
                    confidence="high", why_not="")
    final, _, _ = run("real_oracle_cdc_terminated.json", model=FakeModel(wrong),
                      namespace="MANIMARAN22")

    assert final["tier"] == 3, final["reason"]
    assert "silently skip" in final["reason"], final["reason"]


# =========================================================== SCENARIO 5

def test_restarts_and_verifies_when_it_is_safe():
    """A real Salesforce-to-BigQuery pipeline with a real stored position.

    An API poller cannot re-read a whole dataset, and the checkpoint is
    recorded, so resuming is the right move. Then it is VERIFIED - and the
    verification is about data moving, not about the status word.
    """
    ok = verdict(healthy=False, fault_zone="pipeline",
                 headline="Poller stopped after an API error",
                 likely_cause="the source API refused a call and it halted",
                 restart_would_help=True, confidence="high", why_not="",
                 human_action_needed="", suggested_tier=1)

    final, notifier, _ = run("staged_safe_restart.json", model=FakeModel(ok),
                             namespace="jobregonr")

    assert final["tier"] == 1, (final["tier"], final["reason"])
    assert final["action_taken"] == f"RESUME {SALESFORCE};", final["action_taken"]
    assert final["verification"]["recovered"] is True, final["verification"]
    assert slack_io.RESOLVED in notifier.states, notifier.states

    detail = final["verification"]["detail"]
    assert "wrote new data" in detail or "checkpoint advanced" in detail, detail


# =========================================================== SCENARIO 6

def test_restart_that_does_not_work_is_escalated():
    """A defensible restart that simply did not hold."""
    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="", human_action_needed="",
                 likely_cause="the source API refused a call and it halted")

    final, notifier, _ = run("staged_restart_fails.json", model=FakeModel(ok),
                             namespace="jobregonr")

    assert final["tier"] == 1
    assert final["verification"]["recovered"] is False, final["verification"]
    assert "did NOT work" in notifier.everything, notifier.everything

    # The handover is ON THE PARENT, not underneath it. tier 1 means the
    # agent was the one acting, so nothing else would have said this.
    assert "needs a person" in notifier.final, notifier.final
    assert "the source API refused a call" in notifier.final, notifier.final
    assert final["final_shape"] == "failed", final.get("final_shape")

    # The parent walked 🔧 acting -> 🔴, and never showed ✅.
    assert notifier.states == [slack_io.ACTING, slack_io.RAISED], \
        notifier.states

    # And it says plainly that it tried, rather than pretending it did not.
    assert "Tried it, and it did not hold" in notifier.everything


# =========================================================== SCENARIO 7

def test_same_fault_twice_in_an_hour_is_refused():
    """A repeat is a pattern, and patterns need people."""
    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="", human_action_needed="",
                 likely_cause="the source API refused a call and it halted")

    shared = act.Ledger(path=_SCRATCH / "ledger-repeat.json")
    shared.clear()

    first, _, _ = run("staged_restart_fails.json", model=FakeModel(ok),
                      ledger=shared, namespace="jobregonr")
    assert first["tier"] == 1, first["reason"]

    second, _, _ = run("staged_restart_fails.json", model=FakeModel(ok),
                       ledger=shared, namespace="jobregonr")
    assert second["tier"] == 3, (second["tier"], second["reason"])
    assert "already been acted on" in second["reason"], second["reason"]
    assert second.get("action_result") is None


def test_restart_budget_stops_a_loop():
    """Even distinct causes are capped. Two an hour, then a human."""
    shared = act.Ledger(path=_SCRATCH / "ledger-budget.json")
    shared.clear()
    for n in range(config.MAX_RESTARTS_PER_HOUR):
        shared.record(SALESFORCE, "RESUME", f"cause-{n}", live=False)

    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="",
                 likely_cause="something entirely different this time")
    final, _, _ = run("staged_safe_restart.json", model=FakeModel(ok),
                      ledger=shared, namespace="jobregonr")

    assert final["tier"] == 3, final["reason"]
    assert "budget" in final["reason"], final["reason"]


# =========================================================== SCENARIO 8

def test_foreign_pipeline_is_diagnosed_but_never_touched():
    """Reads are cluster-wide. Writes are not.

    This is also the connector-independence test: a real Oracle CDC source
    in somebody else's namespace, with no DESCRIBE captured for it, still
    profiled correctly from its component name alone.
    """
    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="",
                 headline="Oracle redo logs have been purged",
                 likely_cause="the stored SCN is no longer in the redo logs")

    final, notifier, _ = run("real_oracle_cdc_terminated.json",
                             model=FakeModel(ok), one_shot=True)

    assert final["tier"] == 3, (final["tier"], final["reason"])
    assert "outside sarah_moolai" in final["reason"], final["reason"]
    assert final.get("action_taken") is None
    assert len(notifier.posts) == 1              # still fully diagnosed

    flow = final["evidence"]["flow"]
    assert flow["source"]["profile"]["behaviour"] == "cdc_reader", flow
    assert flow["target"]["profile"]["behaviour"] == "append_only_writer", flow


def test_the_action_layer_refuses_foreign_namespaces_too():
    """Belt and braces: even if DECIDE is wrong, the write path says no."""
    striim = st.Striim.offline(FIXTURES / "real_oracle_cdc_terminated.json")
    try:
        act.run_command(striim, ORACLE, "RESUME")
    except act.ActionRefused as error:
        assert "outside sarah_moolai" in str(error), error
    else:
        raise AssertionError("the action layer allowed a foreign namespace")


# =========================================================== SCENARIO 9

def test_a_lost_verdict_never_discards_the_evidence():
    """THE blocker, observed live.

    Five rounds, ten well-chosen tool calls, and then an empty text block -
    the model spent its output budget thinking, or stopped at max_tokens
    before writing the JSON. The agent reported "could not work out what is
    wrong" over evidence that was sitting right there, already gathered.

    It must report what the tools found. Not a guessed cause - the
    reasoning is what went missing and nothing replaces that - but the
    facts, which are worth far more than an admission of defeat.
    """
    empty = Response([Block("text", text="")], stop_reason="max_tokens")
    final, notifier, _ = run("trial5_halt.json", model=FakeModel(empty))

    incident = notifier.everything
    assert "could not work out what is wrong" not in incident, incident

    # It names what it actually found.
    assert "byte 171,000" in incident, incident
    assert "AI.csv" in incident, incident

    # Composed by code, and says so, and can never become an action.
    assert final["verdict"]["_composed_by_code"] is True
    assert final["verdict"]["confidence"] == "low"
    assert final["verdict"]["likely_cause"].startswith("unknown")
    assert final["tier"] == 3, final["reason"]
    assert final.get("action_taken") is None


def test_the_verdict_is_asked_for_again_before_anything_is_given_up():
    """One more call, with the tools taken away so it cannot wander."""
    empty = Response([Block("text", text="")], stop_reason="max_tokens")
    good = verdict(headline="Source file went missing")
    model = FakeModel(empty, good)

    final, _, _ = run("trial5_halt.json", model=model)

    assert model.calls == 2, model.calls
    # The retry carried no tools at all - it physically cannot look again.
    assert "tools" not in model.last_kwargs, model.last_kwargs.keys()
    assert final["verdict"]["headline"] == "Source file went missing"
    assert final["verdict"].get("_composed_by_code") is None


def test_the_tool_payload_is_trimmed_as_the_history_grows():
    """Ten results at 6,000 characters is 60,000 before the answer starts."""
    messages = [{"role": "user", "content": "opening"}]
    for n in range(6):
        messages.append({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": f"t{n}",
             "content": "x" * 6000}]})

    nodes._trim(messages, keep_full=3, shrink_to=1200)
    sizes = [part["content"] for message in messages
             if isinstance(message["content"], list)
             for part in message["content"]]

    assert all(len(s) < 1400 for s in sizes[:3]), [len(s) for s in sizes]
    assert all(len(s) == 6000 for s in sizes[-3:]), [len(s) for s in sizes]
    assert "trimmed from 6,000 chars" in sizes[0]


def test_malformed_prose_still_yields_the_facts():
    garbage = Response([Block("text", text="I think the pipeline is sad?")])
    final, notifier, _ = run("trial5_halt.json", model=FakeModel(garbage))
    assert final["verdict"]["_composed_by_code"] is True
    assert final["tier"] == 3
    assert "171,000" in notifier.everything


def test_with_nothing_gathered_at_all_it_still_says_so():
    """The fallback reports facts; with no facts it does not invent any."""
    assert nodes.verdict_from_evidence({"app": "ns.X", "evidence": {}}) is None


def test_unknown_fault_zone_is_treated_as_source():
    """Uncertainty resolves towards the safest zone, never the handiest."""
    vague = verdict(fault_zone="banana", restart_would_help=True,
                    confidence="high", why_not="")
    final, _, _ = run("staged_safe_restart.json", model=FakeModel(vague),
                      namespace="jobregonr")
    assert final["fault_zone"] == "unknown", final["fault_zone"]
    assert final["tier"] == 3, final["reason"]


def test_the_model_saying_healthy_about_a_halt_goes_to_a_human():
    """Striim says broken, the model says fine. Trust the control plane."""
    denial = verdict(healthy=True, why_not="", human_action_needed="")
    final, _, _ = run("trial5_halt.json", model=FakeModel(denial))
    assert final["tier"] == 2, final["reason"]
    assert "Striim reports HALT" in final["reason"], final["reason"]


def test_no_model_configured_still_escalates():
    """No API key is not a reason to stop watching."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    services = nodes.Services(striim=striim, model=None, notifier=Recorder(),
                              ledger=act.Ledger(path=_SCRATCH / "l2.json"),
                              evidence_wait=0, evidence_attempts=0,
                              verify_wait=0, verbose=False)
    final = build_graph(services).invoke(new_incident(TRIAL5))

    # No API key is not a reason to say nothing useful: the tools still
    # ran, so the incident still reports what they found.
    assert final["tier"] == 3, final["reason"]
    assert final["verdict"]["_composed_by_code"] is True
    assert "AI.csv" in final["verdict"]["what_happened"]


# ========================================================== SCENARIO 10

def test_slack_being_down_does_not_stop_the_agent():
    """Delivery is never load-bearing.

    A Notifier whose underlying Slack client raises on every single call.
    The class already swallows those, so this proves the run completes and
    the operator still sees the incident.
    """

    class Broken(slack_io.Notifier):
        def __init__(self):
            super().__init__(token=None, channel="#test", echo=False)
            self.printed = []

            class Exploding:
                def chat_postMessage(self, **kwargs):
                    raise RuntimeError("connection reset")

                def chat_update(self, **kwargs):
                    raise RuntimeError("connection reset")

                def reactions_add(self, **kwargs):
                    raise RuntimeError("connection reset")

                def reactions_remove(self, **kwargs):
                    raise RuntimeError("connection reset")

            self.client = Exploding()

        def _say(self, text):
            self.printed.append(text)

    broken = Broken()
    final, _, _ = run("trial5_halt.json", notifier=broken)

    assert final["tier"] == 3, final["reason"]
    assert any("Source file went missing" in line for line in broken.printed), \
        broken.printed
    assert any("Slack delivery failed" in line for line in broken.printed)


# ====================================== THE CONTRACT WITH THE REAL RECORD
#
# The class of test that would have caught all six field-name bugs the
# first suite missed. Every tool, against a real capture, must return the
# keys it documents - with values of the right SHAPE, not merely present.

def test_the_contract_get_status():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.get_status(striim, TRIAL5)
    for key in ("app", "status", "status_means", "has_ever_run", "is_fault",
                "throughput_rate", "definition_edited_at"):
        assert key in out, key
    assert out["status"] == "HALT"
    assert isinstance(out["is_fault"], bool)
    assert isinstance(out["definition_edited_at"], str)


def test_the_contract_get_flow_stats():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.get_flow_stats(striim, TRIAL5)

    # The documented convention is <App>_<Adapter><Role>. The real target
    # is sarah_moolai.File_Trial5_Target, with the app name in the MIDDLE.
    assert out["source"]["component"] == f"{TRIAL5}_FileReaderSource"
    assert out["target"]["component"] == "sarah_moolai.File_Trial5_Target"

    assert isinstance(out["source"]["seconds_since_last_event"], int)
    assert out["source"]["has_ever_read"] is True

    # lagEnd2End is a nested statistics blob, not a number.
    lag = out["target"]["end_to_end_lag"]
    assert isinstance(lag, dict) and "latest" in lag, lag
    assert isinstance(lag["latest"], (int, float)), lag


def test_the_contract_get_checkpoints():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.get_checkpoints(striim, TRIAL5)

    # checkpointInformation is a LIST of {firstRecordedCheckpoint} /
    # {lastTwoRecordedCheckpoints}, and carries TIMES but never positions.
    raw = striim.health()["appHealthMap"][TRIAL5]["checkpointInformation"]
    assert isinstance(raw, list), type(raw)

    assert out["recovery_enabled"] is True
    assert out["has_usable_checkpoint"] is True
    assert out["last_checkpoint_time"] == "2026-08-03 20:04:48"
    assert "never WHERE" in out["note"]


def test_the_contract_describe_component():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.describe_component(striim, f"{TRIAL5}_FileReaderSource")

    assert out["adapter"] == "FileReader"
    # Property keys come back lower-cased from the real endpoint.
    assert "directory" in out["properties"]
    assert out["expects"] == "UploadedFiles/sarah_moolai/AI.csv"

    position = out["read_position"]
    assert position["source_name"] == "AI.csv"
    # Offsets are strings with thousands separators: "170,969".
    assert position["offset_begin_bytes"] == 170969, position
    assert position["offset_end_bytes"] == 171000, position
    assert position["record_length"] == 31, position


def test_the_contract_errors_and_history_on_a_silent_cluster():
    """The real cluster publishes no issue text and no app transitions."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    record = striim.health()
    assert record["issuesList"] == []
    assert not [c for c in record["stateChangeList"]
                if not str(c.get("fqName", "")).startswith("Global.")]

    errors = st.get_errors(striim, TRIAL5)
    assert errors["error_count"] == 0
    assert errors["cluster_publishes_errors"] is False
    assert "says nothing specific about this pipeline" in errors["note"]


def test_the_contract_list_apps_sees_the_whole_cluster():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    everything = st.list_apps(striim, None)
    assert everything["count"] == 576, everything["count"]
    assert everything["by_status"]["HALT"] == 228
    assert everything["by_status"]["TERMINATED"] == 61

    mine = st.list_apps(striim, "sarah_moolai")
    assert mine["count"] == 4, mine


def test_a_component_name_that_matches_nothing_says_so():
    """Some real apps name components with no reference to the app at all."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    flow = st.get_flow_stats(striim,
                             "bharath_qatest_c4aapp.Apporacle19cpostgres12sanity")
    assert flow["source"]["component"] is None
    assert "cannot see them" in flow["note"], flow["note"]


def test_component_matching_does_not_confuse_similar_names():
    """Trial5 must not pick up Trial50 on a cluster with 576 pipelines."""
    record = {"targetHealthMap": {
        "ns.File_Trial5_Target": {},
        "ns.File_Trial50_Target": {},
        "ns.Trial5Extra_Target": {},
    }}
    found = st._components_for(record, "ns.Trial5", "targetHealthMap")
    assert list(found) == ["ns.File_Trial5_Target"], list(found)


# ================================================== DATABASE ACCESS
#
# There is no Postgres here, and there never will be in CI. So the tests
# below run against sqlite3 from the standard library, injected through the
# single seam `db.CONNECT`. That is a real SQL parser and a real
# transaction: a count query derived from the wrong predicate genuinely
# returns the wrong number, and a rollback genuinely rolls back. Stubbing
# `count_rows` to return 4 would test nothing.
#
# What sqlite cannot model - information_schema, pg_replication_slots,
# ctid - is covered by statement-keyed fixtures instead, which fail loudly
# if the code emits a different statement.

import contextlib                                                # noqa: E402
import sqlite3                                                   # noqa: E402


@contextlib.contextmanager
def sqlite_backed(rows=None, table="orders", url="postgresql://test/db",
                  source_url=None):
    """Point db.CONNECT at a real in-memory SQL engine."""
    shared = sqlite3.connect(":memory:")
    shared.execute(f"CREATE TABLE {table} "
                   f"(id INTEGER, batch_id TEXT, name TEXT)")
    for row in rows or []:
        shared.execute(f"INSERT INTO {table} VALUES (?,?,?)", row)
    shared.commit()

    class Handle:
        """One shared database, handed out per 'connection'."""

        def __init__(self, readonly):
            self.readonly = readonly

        def cursor(self):
            return shared.cursor()

        def commit(self):
            shared.commit()

        def rollback(self):
            shared.rollback()

        def close(self):
            pass          # the real one closes; this is shared across calls

    original, t_url, s_url = db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL
    db.CONNECT = lambda u, readonly=False: Handle(readonly)
    config.TARGET_DB_URL = url
    config.SOURCE_DB_URL = source_url
    try:
        yield shared
    finally:
        db.CONNECT, config.TARGET_DB_URL = original, t_url
        config.SOURCE_DB_URL = s_url
        shared.close()


@contextlib.contextmanager
def db_that_raises(error, target="postgresql://test/db"):
    original, t_url = db.CONNECT, config.TARGET_DB_URL

    def explode(url, readonly=False):
        raise error

    db.CONNECT = explode
    config.TARGET_DB_URL = target
    try:
        yield
    finally:
        db.CONNECT, config.TARGET_DB_URL = original, t_url


def test_scenario_6_no_database_and_the_agent_still_works():
    """Acceptance 6, tightened.

    Every db tool must still answer, must not crash, and must never say the
    dead-end sentence "no database access configured" - which tells an
    operator something is missing without telling them what to do. It says
    what it found and what is needed instead.
    """
    assert config.TARGET_DB_URL is None and config.SOURCE_DB_URL is None
    db.forget()

    for name, call in (("db_probe", lambda: db.db_probe("target")),
                       ("db_table_exists",
                        lambda: db.db_table_exists("target", "public.orders")),
                       ("db_row_count",
                        lambda: db.db_row_count("target", "public.orders")),
                       ("db_describe_table",
                        lambda: db.db_describe_table("source", "public.src")),
                       ("db_replication_slot_status",
                        lambda: db.db_replication_slot_status())):
        out = call()
        assert out["configured"] is False, (name, out)
        note = out["note"]
        assert note and len(note) > 40, (name, note)
        assert "no database access configured" not in note, (name, note)
        # It says either where to point it, or what it discovered.
        assert ("DB_URL" in note or "STRIIM_DBPASS" in note
                or "connection URL" in note), (name, note)

    # ...and a full run over a fixture still completes and posts.
    final, notifier, _ = run("trial5_halt.json")
    assert final["tier"] == 3
    assert len(notifier.posts) == 1
    assert final["evidence"]["target_db"]["configured"] is False


# The exact properties a real DatabaseWriter returns. Host, port, database
# and user are plaintext; the password is a Java object handle, which is
# not a password and is not meant to be one.
LIVE_WRITER = {
    "component": "sarah_moolai.Traill_Target",
    "properties": {
        "adaptername": "DatabaseWriter",
        "connectionurl": "jdbc:postgresql://ep-spring-mouse-axhfux81.c-4."
                         "us-east-2.aws.neon.tech:5432/neondb?sslmode=require",
        "username": "neondb_owner",
        "password": "com.webaction.security.PasswordPlatform@173fb898",
        "password_encrypted": True,
    },
}
LIVE_HOST = "ep-spring-mouse-axhfux81.c-4.us-east-2.aws.neon.tech"


def test_the_connection_is_discovered_from_the_pipeline_itself():
    """Everything except the password is already in Striim."""
    db.forget()
    endpoint = db.discover(LIVE_WRITER, "target")

    assert endpoint.host == LIVE_HOST
    assert endpoint.port == 5432
    assert endpoint.database == "neondb"
    assert endpoint.user == "neondb_owner"
    assert endpoint.options["sslmode"] == "require"
    assert endpoint.discovered is True
    assert endpoint.usable is False          # no password yet


def test_the_password_object_handle_is_never_mistaken_for_a_password():
    """`com.webaction.security.PasswordPlatform@173fb898` is a Java object.

    Reading it as a credential would produce a connection attempt with a
    nonsense password and a confusing authentication error.
    """
    db.forget()
    endpoint = db.discover(LIVE_WRITER, "target")
    assert endpoint.password == ""
    assert "PasswordPlatform" not in str(endpoint.redacted())
    assert "PasswordPlatform" not in (endpoint.problem or "")


def test_the_missing_password_names_the_variable_and_the_host():
    """Item 4: never a dead end. The exact variable, for the exact host."""
    db.forget()
    problem = db.discover(LIVE_WRITER, "target").problem

    assert "no database access configured" not in problem
    assert LIVE_HOST in problem, problem
    assert "neondb_owner" in problem, problem
    assert "sarah_moolai.Traill_Target" in problem, problem
    assert ("STRIIM_DBPASS_EP_SPRING_MOUSE_AXHFUX81_C_4_US_EAST_2_AWS_"
            "NEON_TECH=" in problem), problem
    assert "STRIIM_DBPASS_DEFAULT" in problem, problem
    assert "cannot be read back" in problem


def test_a_host_becomes_a_variable_name_three_ways():
    names = db.password_variables(LIVE_HOST)
    assert names[0] == f"STRIIM_DBPASS_{LIVE_HOST}"        # as typed
    assert names[1] == ("STRIIM_DBPASS_EP_SPRING_MOUSE_AXHFUX81_C_4_"
                        "US_EAST_2_AWS_NEON_TECH")          # shell-safe
    assert names[2] == "STRIIM_DBPASS_DEFAULT"

    # Every one of them is a legal environment variable name.
    import re as _re
    assert _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", names[1])


def test_any_of_the_three_variables_supplies_the_password():
    for name in db.password_variables(LIVE_HOST):
        db.forget()
        os.environ[name] = f"secret-via-{name[:20]}"
        try:
            endpoint = db.discover(LIVE_WRITER, "target")
            assert endpoint.usable is True, (name, endpoint.problem)
            assert endpoint.password_from == name
            assert endpoint.connect_kwargs()["password"].startswith("secret-")
            assert endpoint.connect_kwargs()["sslmode"] == "require"
        finally:
            del os.environ[name]


def test_db_config_shows_everything_except_the_secret():
    db.forget()
    os.environ["STRIIM_DBPASS_DEFAULT"] = "hunter2"
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    try:
        out = db.db_config(striim, REJECTING)
        rendered = db.describe_config(out)
    finally:
        del os.environ["STRIIM_DBPASS_DEFAULT"]
        db.forget()

    assert "hunter2" not in json.dumps(out), out
    assert "hunter2" not in rendered, rendered

    target = out["endpoints"]["target"]
    assert target["host"] == "ep-cool-darkness-12345.eu-central-1.aws.neon.tech"
    assert target["database"] == "neondb"
    assert target["user"] == "neondb_owner"
    assert target["password"] == "set, from STRIIM_DBPASS_DEFAULT"
    assert target["usable"] is True

    assert out["source_is_read_only"] is True
    assert "READ ONLY, always" in rendered
    assert "neondb_owner" in rendered


def test_an_explicit_url_still_overrides_discovery():
    db.forget()
    db.remember("target", LIVE_WRITER)
    original = config.TARGET_DB_URL
    config.TARGET_DB_URL = "postgresql://someone@elsewhere/db"
    try:
        endpoint = db.endpoint_for("target")
        assert endpoint.url_override == "postgresql://someone@elsewhere/db"
        assert endpoint.connect_kwargs() == {
            "dsn": "postgresql://someone@elsewhere/db"}
    finally:
        config.TARGET_DB_URL = original
        db.forget()


def test_a_non_postgres_endpoint_is_declined_rather_than_guessed_at():
    db.forget()
    oracle = {"component": "ns.App_OracleSource",
              "properties": {"connectionurl":
                             "jdbc:oracle:thin:@ora-prod-01:1521/ORCL",
                             "username": "system"}}
    endpoint = db.discover(oracle, "source")
    assert endpoint.usable is False
    assert "only speaks Postgres" in endpoint.problem, endpoint.problem
    assert "will not query the database" in endpoint.problem


def test_discovery_does_not_leak_between_pipelines():
    """The supervisor watches several apps in one process.

    A connection discovered for one must never still be there when the
    next is diagnosed - that is the agent querying somebody else's
    database and reporting the answer confidently.
    """
    db.forget()
    db.remember("target", LIVE_WRITER)
    assert db.endpoint_for("target").host == LIVE_HOST

    evidence_for("trial5_halt.json")          # a different app entirely
    found = db.endpoint_for("target")
    assert found is None or found.host != LIVE_HOST, found


def test_every_tool_is_reachable_from_the_command_line():
    """`reconcile` existed, had a schema, was offered to the model — and
    answered "No such tool" from the CLI, because the dispatcher knew only
    one of the two registries.

    It is the tool that answers this project's own central finding, and it
    was unreachable by the only person who would run it by hand.
    """
    from agent import toolbox

    assert "reconcile" in toolbox.names()
    assert "db_compare_schema" in toolbox.names()
    assert toolbox.needs_striim("get_status") is True
    assert toolbox.needs_striim("reconcile") is False

    # Every tool in either registry is callable through the one dispatcher.
    for name in toolbox.names():
        assert name in toolbox.described(), name

    # And the CLI lists them all, not just the Striim half.
    listed = set(toolbox.described())
    assert listed >= set(st.TOOLS) | set(db.TOOLS)


def test_the_dispatcher_reaches_both_registries():
    from agent import toolbox
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    from_striim = toolbox.call("get_status", striim, TRIAL5)
    assert from_striim["status"] == "HALT"

    from_db = toolbox.call("db_probe", striim, "target")
    assert from_db["configured"] is False       # no URL here, but it ran

    try:
        toolbox.call("no_such_tool", striim)
    except KeyError:
        pass
    else:
        raise AssertionError("the dispatcher accepted an unknown tool")


def test_a_table_existing_is_not_a_schema_matching():
    """The live miss: a column was dropped, the table still existed,
    db_table_exists said true, and the agent stopped looking."""
    rows = {
        ("public", "source"): [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "name", "data_type": "text"},
            {"column_name": "city", "data_type": "text"}],
        ("public", "target"): [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "name", "data_type": "text"}],
    }

    original, t_url, s_url = (db.CONNECT, config.TARGET_DB_URL,
                              config.SOURCE_DB_URL)
    db.CONNECT = lambda info, readonly=False: _schema_rows(rows)
    config.TARGET_DB_URL = "postgresql://t/db"
    config.SOURCE_DB_URL = "postgresql://s/db"
    try:
        out = db.db_compare_schema("public.source", "public.target")
    finally:
        db.CONNECT, config.TARGET_DB_URL = original, t_url
        config.SOURCE_DB_URL = s_url

    assert out["comparable"] is True
    assert out["matches"] is False
    assert out["missing_in_target"] == ["city"]
    assert "the target is MISSING city (text)" in out["note"], out["note"]
    assert "an existence check passes" in out["note"]

    # The repair comes out of the diff, with the type from the source.
    assert out["repair"] == [
        'ALTER TABLE "public"."target" ADD COLUMN "city" text;'], out["repair"]


def _schema_rows(by_table):
    """A connection that answers information_schema queries from a map."""
    class Cursor:
        description = None
        _rows = []

        def execute(self, sql, params=()):
            if "constraint_type" in sql:
                self.description = [("column_name",)]
                self._rows = [("id",)]
                return self
            columns = by_table.get(tuple(params[:2]), [])
            self.description = [("column_name",), ("data_type",),
                                ("is_nullable",), ("column_default",),
                                ("is_identity",), ("identity_generation",)]
            self._rows = [(c["column_name"], c["data_type"], "YES",
                           None, "NO", None) for c in columns]
            return self

        def fetchall(self):
            return self._rows

    class Handle:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    return Handle()


def test_a_schema_that_was_never_compared_is_not_a_schema_that_matches():
    """The live wrong answer, reproduced.

    A column was dropped from the TARGET. db_table_exists said the table
    was there, the comparison silently did not run, no schema_diff appeared
    in the evidence at all - and the diagnosis concluded "the fault is in
    the source, which this agent never writes to", from both ends being
    quiet. Reasonable inference from missing evidence; wrong conclusion.

    An absent check must not look like a passed one.
    """
    # 1. The comparison itself, when the source side cannot be read.
    original = db.CONNECT
    db.CONNECT = None                      # nothing reachable either side
    try:
        out = db.db_compare_schema(None, "public.target")
    finally:
        db.CONNECT = original

    assert out["checked"] is False, out
    assert out["matches"] is None, out          # NOT False, and NOT True
    assert "WERE NOT COMPARED" in out["note"], out["note"]
    assert "NOT 'they match'" in out["note"]
    assert "do not place the fault elsewhere" in out["note"]

    # 2. The guardrail, which does not depend on the model reading that.
    ev = {"target_table": {"table": "public.target", "exists": True},
          "schema_diff": {"checked": False, "matches": None}}
    warning = nodes._schema_not_compared(ev)
    assert warning and "never compared" in warning, warning
    assert "do not treat the target as ruled out" in warning

    # 3. And a source verdict reached without that check says so.
    state = {"app": TRIAL5, "status": "HALT", "evidence": ev,
             "verdict": verdict(fault_zone="source",
                                why_not="the source went quiet")}
    services = services_for("trial5_halt.json")
    out = nodes.decide(state, services)
    assert out["tier"] == 3
    assert "never compared" in out["reason"], out["reason"]


def test_the_schema_is_compared_even_when_the_source_table_is_unknown():
    """It used to be gated on resolving the source table, and that gate is
    what made the check disappear."""
    ev = evidence_for("trial5_halt.json")
    # No database configured here, so it cannot compare - but the ATTEMPT
    # has to be recorded, because "did not check" is itself evidence.
    if (ev.get("target_table") or {}).get("exists"):
        assert "schema_diff" in ev, ev.keys()
        assert ev["schema_diff"]["checked"] is False


def test_table_exists_does_not_read_as_a_clean_bill_of_health():
    rows = {("public", "target"): [{"column_name": "id",
                                    "data_type": "integer"}]}
    original, t_url = db.CONNECT, config.TARGET_DB_URL
    db.CONNECT = lambda info, readonly=False: _exists_row()
    config.TARGET_DB_URL = "postgresql://t/db"
    try:
        out = db.db_table_exists("target", "public.target")
    finally:
        db.CONNECT, config.TARGET_DB_URL = original, t_url

    assert out["exists"] is True
    assert "THAT IS ALL THIS MEANS" in out["note"], out["note"]
    assert "not evidence that the target is" in out["note"]
    assert "db_compare_schema" in out["note"]

    # ...and the model is told the same thing.
    assert "A TABLE EXISTING IS NOT A TARGET BEING HEALTHY" in prompts.SYSTEM
    assert "DO NOT PLACE A FAULT IN THE SOURCE BY ELIMINATION" in prompts.SYSTEM


def _exists_row():
    class Cursor:
        description = [("table_schema",), ("table_name",)]

        def execute(self, sql, params=()):
            return self

        def fetchall(self):
            return [("public", "target")]

    class Handle:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    return Handle()


def test_the_live_scenario_a_dropped_column_is_repaired_end_to_end():
    """THE one that has to work. The live cluster, right now.

    A column exists on the source and not on the target, so every write
    fails. There is no error text - this deployment publishes none for any
    application. STRIIM_LIVE=1 and DDL execution is approved.

    Expected: the skill fires from the schema difference alone, the ALTER
    executes, the pipeline resumes, and the incident reads as a repair
    rather than a request.
    """
    ran = []

    class Applied:
        """A target that records DDL and then reports the column present."""

        def cursor(self):
            return self

        description = None
        rowcount = -1          # psycopg2's "unknown", which DDL returns

        def execute(self, sql, params=()):
            self.description = None
            if sql.strip().upper().startswith("ALTER"):
                ran.append(sql)
                return self
            if "information_schema.columns" in sql:
                target_side = params and params[1] == "target"
                columns = [("id", "integer"), ("name", "text")]
                if not target_side or ran:      # 'age' appears once added
                    columns.append(("age", "integer"))
                self.description = [("column_name",), ("data_type",),
                                    ("is_nullable",), ("column_default",),
                                    ("is_identity",), ("identity_generation",)]
                self._rows = [(n, t, "YES", None, "NO", None)
                              for n, t in columns]
                return self
            if "constraint_type" in sql:
                self.description = [("column_name",)]
                self._rows = [("id",)]
                return self
            if "information_schema.tables" in sql:
                self.description = [("table_schema",), ("table_name",)]
                self._rows = [("public", "target")]
                return self
            if "pg_replication_slots" in sql:
                # Once the repair has run and the pipeline is reading
                # again, the slot advances - which is what verification
                # now checks, rather than the fact that a checkpoint
                # happened.
                lsn = "0/3A2B1C0" if not ran else "0/3A2F400"
                self.description = [("slot_name",), ("plugin",),
                                    ("slot_type",), ("active",),
                                    ("restart_lsn",), ("confirmed_flush_lsn",)]
                self._rows = [("striim_slot", "pgoutput", "logical", True,
                               "0/3A2B1C0", lsn)]
                return self
            self.description = [("?column?",)]
            self._rows = [(1,)]
            return self

        def fetchall(self):
            return self._rows

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    verdict_for_it = verdict(
        healthy=False, fault_zone="target", confidence="high",
        headline="Target is missing a column",
        what_happened="Writes fail because the target has no age column.",
        likely_cause="the age column was dropped from the target",
        restart_would_help=False, why_not="", human_action_needed="",
        suggested_skill="recreate_missing_target_object")

    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL,
                 config.DRY_RUN, config.NAMESPACE)
    db.CONNECT = lambda info, readonly=False: Applied()
    config.TARGET_DB_URL = "postgresql://t/neondb"
    config.SOURCE_DB_URL = "postgresql://s/neondb"
    config.DRY_RUN = False                       # STRIIM_LIVE=1
    config.NAMESPACE = "sarah_moolai"
    try:
        final, notifier, _ = run("rejected_rows.json",
                                 model=FakeModel(verdict_for_it),
                                 ledger=act.Ledger(path=_SCRATCH / "l-ddl.json"))
    finally:
        (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL,
         config.DRY_RUN, config.NAMESPACE) = originals
        db.forget()

    # The schema difference was measured, with no error text anywhere.
    diff = final["evidence"]["schema_diff"]
    assert diff["checked"] is True
    assert diff["missing_in_target"] == ["age"], diff
    assert final["evidence"]["errors"]["error_count"] == 0

    # The skill fired, rather than declining for want of error text.
    assert final["skill"] == "recreate_missing_target_object", \
        final.get("skill_dropped")
    assert final["tier"] == 1, (final["tier"], final["reason"])

    # The DDL actually ran, and it was the derived statement.
    assert ran == ['ALTER TABLE "public"."target" ADD COLUMN "age" integer'], ran

    # Then the pipeline was resumed, and it verified.
    assert any("resumed" in d for d in final["did"]), final["did"]
    assert final["verification"] is not None

    # And the incident reads as a repair, not a request.
    assert final["final_shape"] == "fixed", final["final_shape"]
    assert "— fixed" in notifier.final, notifier.final
    assert "I did:" in notifier.final
    assert "age" in notifier.final
    assert "Run this on" not in notifier.final, notifier.final


# ===================================== HEDGING, AND THE POLLING LOOP

def test_a_conditional_about_a_checkable_fact_is_caught():
    """Published live, twice in one run, by something holding the tools.

        "If the replication slot is gone…"
        "If the file is still missing…"

    Both are questions. Both are one call away. Handing them to a reader
    at 3am hands them to whoever has LESS access than the sender.
    """
    hedged = verdict(why_not="If the replication slot is gone, restarting "
                             "would re-read everything")
    found = prompts.unresolved_conditionals(hedged)
    assert found, "a conditional about the slot went unnoticed"
    assert found[0]["tool"] == "db_replication_slot_status"
    assert found[0]["field"] == "why_not"

    for text, tool in (
            ("if the target table was dropped, this will fail",
             "db_table_exists"),
            ("the column may have been removed from the target",
             "db_compare_schema"),
            ("possibly a permission was revoked", "db_check_privileges"),
            ("whether rows are being rejected is unclear",
             "get_rejected_events")):
        found = prompts.unresolved_conditionals(
            verdict(human_action_needed=text))
        assert found and found[0]["tool"] == tool, (text, found)


def test_a_conditional_about_something_unreachable_is_left_alone():
    """Hedging is fine about things no tool can settle."""
    fine = verdict(why_not="if somebody restarts the source database during "
                           "the maintenance window this will need redoing")
    # No checkable subject in there - nothing to go and look up.
    assert prompts.unresolved_conditionals(fine) == []

    plain = verdict(why_not="the replication slot is gone, so there is no "
                            "position to resume from")
    assert prompts.unresolved_conditionals(plain) == [], "a statement of "\
        "fact was mistaken for a hedge"


def test_a_hedged_verdict_is_settled_and_re_asked():
    """The tool is called HERE, not suggested - the model already had its
    chance to call it and wrote a conditional instead."""
    hedged = verdict(why_not="If the replication slot is gone, a restart "
                             "would duplicate everything")
    firm = verdict(why_not="the replication slot striim_slot no longer "
                           "exists, so a restart re-reads from the start")

    model = FakeModel(hedged, firm)
    final, _, _ = run("rejected_rows.json", model=model, app=REJECTING)

    assert model.calls == 2, model.calls
    assert final["verdict"]["why_not"].startswith("the replication slot")

    # It went and looked before asking again.
    assert any("settling a conditional" in t for t in final["trace"]), \
        final["trace"]

    # ...and the answer was put in front of the model before it re-answered.
    sent = "\n".join(str(m.get("content")) for m in
                     model.last_kwargs["messages"] if m.get("role") == "user")
    assert "You wrote a conditional about something you can check" in sent
    assert "may begin with 'if'" in sent

    # Once only. A second hedge is not chased round for ever.
    assert final["rounds"] <= config.MAX_ROUNDS


def test_the_prompt_forbids_publishing_a_checkable_conditional():
    assert "NEVER PUBLISH A CONDITIONAL ABOUT SOMETHING YOU CAN CHECK" \
        in prompts.SYSTEM
    assert "handing it to the wrong person" in prompts.SYSTEM


def test_there_is_no_apology_line():
    """"the agent could not determine what is wrong with it from here" is
    a shrug with a full stop after it, and it appeared on incidents whose
    cause was perfectly well established."""
    import pathlib
    package = pathlib.Path(__file__).resolve().parents[1] / "agent"
    for path in package.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "could not determine what is wrong with it from here" \
            not in text, path

    # What replaces it names the unknown and the command that settles it.
    action = skills._source_action("public.source", None, {})
    assert "SELECT count(*) FROM public.source" in action, action


def test_the_status_sweep_is_not_blocked_by_an_investigation():
    """Live: one pipeline checked, two at "not checked yet" for minutes,
    because a faulted pipeline ran a whole investigation - model call,
    evidence wait, 210-second verification - before the loop moved on."""
    services = services_for("trial5_halt.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-sweep.json"))
    registry = Registry(path=_SCRATCH / "reg-sweep.json",
                        namespace="sarah_moolai")
    registry.replace([TRIAL5, TRIAL3, "sarah_moolai.Trial1"])
    supervisor = Supervisor(services, registry)

    slow = []

    class Slow:
        def invoke(self, state):
            slow.append(state["app"])
            time.sleep(0.4)          # a real one takes minutes
            return {"status": "HALT"}

    supervisor.graph = Slow()

    started = time.monotonic()
    seen = supervisor.tick()
    elapsed = time.monotonic() - started

    # Every pipeline has a status, immediately, and none of them waited.
    assert set(seen) == set(registry.all()), seen
    assert all(v != "not checked yet" for v in seen.values()), seen
    assert elapsed < 0.3, f"the sweep waited for an investigation ({elapsed:.1f}s)"

    # `status` never says "not checked yet" once a cycle has run.
    answer = supervisor.status()
    assert "not checked yet" not in answer, answer
    for app in registry.all():
        assert app in answer

    supervisor._queued.clear()


def test_one_investigation_at_a_time_and_no_duplicates():
    services = services_for("trial5_halt.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-queue.json"))
    registry = Registry(path=_SCRATCH / "reg-queue.json",
                        namespace="sarah_moolai")
    registry.replace([TRIAL5])
    supervisor = Supervisor(services, registry)

    ran = []
    supervisor.graph = type("G", (), {
        "invoke": lambda self, state: ran.append(state["app"]) or
                  {"status": "HALT"}})()
    supervisor.check_drift = lambda app, force=False: None

    # Queued twice before the worker runs - it should appear once.
    assert supervisor.investigate_later(TRIAL5) is True
    assert supervisor.investigate_later(TRIAL5) is False
    supervisor.drain_now()

    assert ran == [TRIAL5], ran
    assert supervisor._working_on is None


# ================================================== SCHEMA DRIFT
#
# With schema evolution set to Ignore, a source DDL is captured and
# deliberately not forwarded. Nothing breaks. The target falls behind
# silently until some write happens to need the missing column - which can
# be hours later and looks like an unrelated fault by then.

def test_the_ignored_ddl_counter_is_the_earliest_warning_there_is():
    """It ticks the moment somebody alters the source."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    original = st.mon_app
    st.mon_app = lambda s, app: {
        "shape": "structured", "statement": f"MON {app};",
        "output": [{"DDLs Ignored": "3",
                    "Last DDL Captured": 'ALTER TABLE "public"."source" '
                                         'ADD COLUMN "region" text',
                    "Last DDL Time": "2026-08-08 16:25:00"}]}
    try:
        out = st.ddl_metrics(striim, TRIAL5)
    finally:
        st.mon_app = original

    assert out["ignored_ddl_count"] == 3
    assert "region" in out["last_ddl_statement"]
    assert out["last_ddl_at"] == "2026-08-08 16:25:00"
    assert "NOT applied to the target" in out["note"]
    assert "nothing has broken yet" in out["note"]


def test_unreadable_ddl_metrics_are_not_read_as_zero():
    """MON's response shape has never been captured. "I could not look" is
    not "no DDL has been ignored"."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    original = st.mon_app
    st.mon_app = lambda s, app: {"shape": "text", "raw": "+---+",
                                 "statement": f"MON {app};"}
    try:
        out = st.ddl_metrics(striim, TRIAL5)
    finally:
        st.mon_app = original

    assert out["ignored_ddl_count"] is None
    assert "NOT 'no DDL has been ignored'" in out["note"]
    assert "MON sarah_moolai.Trial5;" in out["note"]


def test_drift_warns_and_does_not_raise_an_incident():
    """A warning informs; an incident interrupts. Nothing is broken."""
    report = {
        "app": "sarah_moolai.sarah1_CDC", "drifted": True, "checked": True,
        "findings": [{"kind": "missing_column", "column": "region",
                      "type": "text",
                      "detail": "the source has region and the target does not"},
                     {"kind": "ignored_ddl",
                      "detail": "1 schema change(s) from the source have been "
                                "captured and deliberately not applied to the "
                                "target",
                      "when": time.time() - 1200,
                      "statement": 'ALTER TABLE "public"."source" ADD COLUMN '
                                   '"region" text'}]}

    text = drift.compose(report)

    assert text.startswith("🟡"), text
    assert "nothing is broken yet" in text
    assert "The source gained *region* (text)" in text
    assert "20 minutes ago" in text
    assert "writes carrying that column will fail" in text
    assert "I will repair it automatically when something does" in text

    # Emphatically NOT an incident.
    for marker in (slack_io.RAISED, slack_io.NEEDS_HUMAN, "You need to:",
                   "not safe to restart"):
        assert marker not in text, marker


def test_each_distinct_drift_is_mentioned_once():
    services = services_for("trial5_running.json")
    registry = Registry(path=_SCRATCH / "reg-drift.json",
                        namespace="sarah_moolai")
    registry.replace([TRIAL5])
    supervisor = Supervisor(services, registry)

    report = {"app": TRIAL5, "drifted": True, "checked": True,
              "findings": [{"kind": "missing_column", "column": "region",
                            "type": "text", "detail": "x"}]}

    original = drift.check
    drift.check = lambda striim, app: report
    try:
        supervisor.check_drift(TRIAL5, force=True)
        first = len(services.notifier.posts) + len(
            [m for m in services.notifier.replies])
        said_once = services.notifier
        supervisor.check_drift(TRIAL5, force=True)
        supervisor.check_drift(TRIAL5, force=True)
    finally:
        drift.check = original

    # `say` goes to the channel, not a thread - counted via the echo list.
    assert len(supervisor.drift_warned) == 1

    # A DIFFERENT drift is a different warning.
    report["findings"].append({"kind": "missing_column", "column": "city",
                               "type": "text", "detail": "y"})
    drift.check = lambda striim, app: report
    try:
        supervisor.check_drift(TRIAL5, force=True)
    finally:
        drift.check = original
    assert len(supervisor.drift_warned) == 2


def test_drift_runs_on_a_schedule_not_every_poll():
    services = services_for("trial5_running.json")
    registry = Registry(path=_SCRATCH / "reg-sched.json",
                        namespace="sarah_moolai")
    registry.replace([])
    supervisor = Supervisor(services, registry)

    calls = []
    original = drift.check
    drift.check = lambda striim, app: (calls.append(app) or
                                       {"app": app, "drifted": False,
                                        "checked": True, "findings": []})
    try:
        supervisor.check_drift(TRIAL5)
        supervisor.check_drift(TRIAL5)          # too soon
        assert len(calls) == 1, calls

        supervisor.drift_checked[TRIAL5] -= config.SCHEMA_CHECK_SECONDS + 1
        supervisor.check_drift(TRIAL5)
        assert len(calls) == 2, calls
    finally:
        drift.check = original


def test_a_constraint_only_the_target_enforces_is_found():
    """TASK-5 §2. Rows the source thinks are fine, refused and dropped,
    while the pipeline reports RUNNING."""
    rows = {
        "source": [{"name": "source_pkey", "kind": "p",
                    "definition": "PRIMARY KEY (id)"}],
        "target": [{"name": "target_pkey", "kind": "p",
                    "definition": "PRIMARY KEY (id)"},
                   {"name": "age_check", "kind": "c",
                    "definition": "CHECK ((age < 40))"}],
    }

    class Cursor:
        description = [("name",), ("kind",), ("definition",)]
        _rows = []

        def execute(self, sql, params=()):
            side = "target" if params and params[0] == "target" else "source"
            self._rows = [(r["name"], r["kind"], r["definition"])
                          for r in rows[side]]
            return self

        def fetchall(self):
            return self._rows

    class Handle:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL)
    db.CONNECT = lambda info, readonly=False: Handle()
    config.TARGET_DB_URL = "postgresql://t/db"
    config.SOURCE_DB_URL = "postgresql://s/db"
    try:
        out = db.db_compare_constraints("public.source", "public.target")
    finally:
        db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL = originals

    assert out["checked"] is True
    assert out["matches"] is False
    assert out["rejecting"] == ["age_check"]
    assert "age_check (CHECK) CHECK ((age < 40))" in out["note"]
    assert "keeps running while those rows go to the exception store" \
        in out["note"]
    assert out["repair"] == [
        'ALTER TABLE "public"."target" DROP CONSTRAINT "age_check";']

    # A primary key the target has and the source lacks is ordinary.
    assert "target_pkey" not in out["rejecting"]


def test_constraints_that_could_not_be_read_are_not_reported_as_matching():
    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL)
    db.CONNECT = None
    config.TARGET_DB_URL = config.SOURCE_DB_URL = None
    try:
        out = db.db_compare_constraints("public.source", "public.target")
    finally:
        db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL = originals

    assert out["checked"] is False
    assert out["matches"] is None
    assert "not 'they match'" in out["note"]


# ============================================== DDL APPROVAL IN A THREAD

def _approval_state(services, thread, statement, table="public.target"):
    """A tier-1 target repair sitting on a real incident thread."""
    return {"app": TRIAL5, "status": "HALT", "tier": 1,
            "slack_thread_ts": thread, "fingerprint": "sarah#a1b2",
            "evidence": {"schema_diff": {"checked": True,
                                         "target_table": table}}}


def test_widening_runs_without_asking():
    """Additive, the pipeline is already broken, and nobody is awake."""
    skill = skills.SKILLS["widen_target_column"]
    assert skill.max_tier == 1

    diff = {"checked": True, "matches": False, "target_table": "public.target",
            "differing_types": [{"column": "name", "source_type": "text",
                                 "target_type": "varchar(3)",
                                 "widening": True}]}
    state = {"app": TRIAL5, "evidence": {"schema_diff": diff}}

    ok, why = skill.applies(state)
    assert ok, why
    assert "varchar(3) on the target" in why

    columns = {"target": [_ID, ("name", "character varying", "YES", None,
                                "NO", None, 3, None, None)]}
    original = config.DRY_RUN
    config.DRY_RUN = False
    try:
        with a_catalog(columns):
            plan = skill.plan(state)
            prepared = ddl.plan(plan.command, derived_from=plan.derived_from,
                                incident="i-w")
    finally:
        config.DRY_RUN = original

    assert plan.executable is True, plan.reason
    assert plan.command == ('ALTER TABLE "public"."target" ALTER COLUMN '
                            '"name" TYPE text;')
    assert "information_schema" in plan.derived_from
    # Additive, so no approval and no backup.
    assert prepared["kind"] == ddl.ADDITIVE
    assert prepared["backup_table"] is None


def test_a_target_wider_than_its_source_is_not_widened_back():
    skill = skills.SKILLS["widen_target_column"]
    state = {"app": TRIAL5, "evidence": {"schema_diff": {
        "checked": True, "differing_types": [
            {"column": "name", "source_type": "varchar(3)",
             "target_type": "text", "widening": False}]}}}

    ok, why = skill.applies(state)
    assert ok is False
    assert "not what is rejecting writes" in why, why


def test_a_destructive_repair_asks_in_the_thread_and_waits():
    """DROP NOT NULL removes no data but changes what the table accepts."""
    services = services_for("trial5_halt.json")
    notifier = services.notifier
    thread = notifier.post_incident("🔴 test")

    prepared = {"statement": 'ALTER TABLE "public"."target" ALTER COLUMN '
                             '"region" DROP NOT NULL',
                "kind": ddl.DESTRUCTIVE, "runnable": True,
                "table": "public.target", "columns": ["region"],
                "counts": {"rows": 1204}, "backup_table": "striim_agent_backup_x",
                "restore": 'ALTER TABLE "public"."target" ALTER COLUMN '
                           '"region" SET NOT NULL;',
                "derived_from": "a measured schema diff", "form": "drop_not_null"}

    try:
        nodes.request_approval(_approval_state(services, thread, prepared),
                               services, prepared, "relax_blocking_column")
    except nodes.AwaitingApproval as waiting:
        pending = waiting.pending
    else:
        raise AssertionError("it ran a destructive change unasked")

    assert pending["state"] == "waiting"
    assert services.pending_for(thread) is pending

    # The ask goes on the PARENT, not into the thread. Somebody reading a
    # notification preview must be able to answer without opening anything.
    assert notifier.replies == [], notifier.replies

    asked = slack_io.compose_incident(
        TRIAL5, verdict(),
        nodes.build_record({"app": TRIAL5, "tier": 2, "did": [],
                            "awaiting_approval": prepared["statement"],
                            "approval": nodes._approval_facts(pending),
                            "verdict": verdict(), "evidence": {}}))

    assert "DROP NOT NULL" in asked, asked
    assert "1,204 rows in public.target" in asked, asked
    assert "striim_agent_backup_x" in asked, asked
    assert "Reply *approve*" in asked and "*skip*" in asked, asked
    assert "give up in 15 minutes" in asked, asked
    assert 'SET NOT NULL' in asked, asked          # how to put it back


def test_only_one_pending_approval_per_incident():
    services = services_for("trial5_halt.json")
    thread = services.notifier.post_incident("🔴 test")
    prepared = {"statement": 'ALTER TABLE "public"."t" DROP COLUMN "a"',
                "kind": ddl.DESTRUCTIVE, "runnable": True,
                "table": "public.t", "columns": ["a"], "counts": {},
                "backup_table": "b", "derived_from": "measured",
                "form": "drop_column"}
    state = _approval_state(services, thread, prepared)

    first = second = None
    try:
        nodes.request_approval(state, services, prepared, "s")
    except nodes.AwaitingApproval as waiting:
        first = waiting.pending
    try:
        nodes.request_approval(state, services, prepared, "s")
    except nodes.AwaitingApproval as waiting:
        second = waiting.pending

    assert first is second, "a second request replaced the first"
    assert len(services.pending) == 1


def test_approve_runs_it_and_a_second_approve_does_nothing():
    # Its own ledger: approving really does RESUME the pipeline, and that
    # would spend the restart budget every other trial5 test shares.
    services = services_for("trial5_halt.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-approve.json"))
    registry = Registry(path=_SCRATCH / "reg-approve.json",
                        namespace="sarah_moolai")
    registry.replace([])
    supervisor = Supervisor(services, registry)
    console = slack_io.Console(supervisor, bot_token=None, app_token=None)

    thread = services.notifier.post_incident("🔴 test")
    ran = []
    services.pending[thread] = {
        "app": TRIAL5, "thread": thread, "skill": "s", "fingerprint": "f",
        "plan": {"statement": "x"}, "asked_at": time.time(),
        "state": "waiting", "question": "?", "summary": "DROP COLUMN a",
        "rows": 4, "backup": "b"}

    original = ddl.execute
    ddl.execute = lambda plan, services=None, thread=None: ran.append(plan)
    try:
        answer = console.handle("approve", thread=thread)
        assert "Running it now" in answer, answer
        for worker in list(threading.enumerate()):
            if worker.name.startswith("approved-"):
                worker.join(timeout=5)

        again = console.handle("approve", thread=thread)
    finally:
        ddl.execute = original

    assert len(ran) == 1, ran
    assert "already approved" in again, again


def test_skip_abandons_it_and_touches_nothing():
    services = services_for("trial5_halt.json")
    supervisor = Supervisor(services, Registry(
        path=_SCRATCH / "reg-skip.json", namespace="sarah_moolai"))
    console = slack_io.Console(supervisor, bot_token=None, app_token=None)

    thread = services.notifier.post_incident("🔴 test")
    services.pending[thread] = {
        "app": TRIAL5, "thread": thread, "skill": "s", "fingerprint": "f",
        "plan": {"statement": "x"}, "asked_at": time.time(),
        "state": "waiting", "question": "?", "summary": "DROP COLUMN a",
        "rows": 4, "backup": "b"}

    answer = console.skip if False else console.handle("skip", thread=thread)
    assert "Left alone" in answer
    assert "no backup was taken" in answer
    assert services.pending[thread]["state"] == "skipped"
    assert services.pending_for(thread) is None


def test_an_unanswered_approval_is_abandoned_and_says_so():
    """A repair waiting forever is a silent failure."""
    services = services_for("trial5_halt.json")
    thread = services.notifier.post_incident("🔴 test")
    services.pending[thread] = {
        "app": TRIAL5, "thread": thread, "skill": "s", "fingerprint": "f",
        "plan": {}, "asked_at": time.time() - config.APPROVAL_TIMEOUT_SECONDS - 1,
        "state": "waiting", "question": "?", "summary": "DROP COLUMN a",
        "rows": 4, "backup": "b"}

    expired = services.expire_approvals()
    assert expired == [thread]
    assert services.pending[thread]["state"] == "expired"

    said = "\n".join(services.notifier.replies)
    assert "nobody approved it" in said
    assert "Nothing was changed and no backup was taken" in said

    # And it does not fire twice.
    services.notifier.replies.clear()
    assert services.expire_approvals() == []
    assert services.notifier.replies == []


def test_a_held_repair_is_never_verified_as_an_attempt():
    """THE LIVE FAULT, from state/audit.log on 2026-08-10:

        23:13:56  tier 1, relax_blocking_column, zone target
        23:13:59  approval requested — ALTER TABLE ... DROP NOT NULL
        23:17:36  verification {'recovered': False, 'still in TERMINATED'}

    217 seconds is one VERIFY_WAIT. Nothing was restarted - the repair was
    held for approval - and VERIFY ran anyway, found the pipeline exactly
    as it had been left, and reported that the restart did not work. CLOSE
    then chose the failed shape over the waiting one, so the parent said
    "Tried it, and it did not hold" instead of "Reply *approve*". Nobody
    answered a question they were never shown, and it expired.
    """
    services = services_for("trial5_halt.json")
    notifier = services.notifier
    thread = notifier.post_incident("🔴 provisional")

    verified = []
    services.striim.verify = lambda *a, **k: verified.append(a)

    state = {"app": TRIAL5, "slack_thread_ts": thread, "tier": 2,
             "did": [], "before_snapshot": {}, "closing": True,
             "verdict": verdict(headline="Target column blocks every write"),
             "action_result": {"awaiting_approval": True},
             "awaiting_approval": 'ALTER TABLE "public"."target" ALTER COLUMN '
                                  '"region" DROP NOT NULL',
             "approval": {"counts": {"rows": 5}, "table": "public.target",
                          "backup": "striim_agent_backup_x",
                          "restore": 'ALTER TABLE "public"."target" ALTER '
                                     'COLUMN "region" SET NOT NULL;',
                          "derived_from": "the measured schema difference"}}

    # VERIFY must not run at all - not the 210-second wait, not the reply.
    assert nodes.verify_node(state, services) == {"verification": None}
    assert verified == [], verified
    assert notifier.replies == [], notifier.replies

    # CLOSE must show the ask, not a report of an attempt.
    nodes.close_node(state, services)
    parent = notifier.final

    assert "Reply *approve*" in parent, parent
    assert "DROP NOT NULL" in parent, parent
    assert "5 rows in public.target" in parent, parent
    assert "striim_agent_backup_x" in parent, parent

    for lie in ("Tried it", "did not hold", "did NOT work", "restart"):
        assert lie not in parent, (lie, parent)


def test_a_held_repair_survives_a_stale_verification_in_the_state():
    """Belt and braces: even if a verification is somehow present, waiting
    for a person wins. It describes a pipeline nobody touched."""
    record = nodes.build_record({
        "app": TRIAL5, "tier": 2, "did": [], "closing": True,
        "verdict": verdict(),
        "verification": {"recovered": False, "detail": "still in TERMINATED"},
        "awaiting_approval": 'ALTER TABLE "t" DROP COLUMN "c"',
        "approval": {"counts": {"rows": 5}, "table": "public.target"}})

    assert record["shape"] == "needs_command", record
    assert "approve" in record["command"]["next"], record


def test_an_expired_approval_stops_offering_a_choice_nobody_can_take():
    """The observed fault: 15 minutes after it gave up, the parent still
    read "Reply *approve* to run it ... I will give up in 15 minutes"."""
    services = services_for("trial5_halt.json")
    notifier = services.notifier
    thread = notifier.post_incident("🔴 provisional")

    prepared = {"statement": 'ALTER TABLE "public"."target" ALTER COLUMN '
                             '"region" DROP NOT NULL',
                "kind": ddl.DESTRUCTIVE, "runnable": True,
                "table": "public.target", "columns": ["region"],
                "counts": {"rows": 5}, "backup_table": "striim_agent_backup_x",
                "restore": 'ALTER TABLE "public"."target" ALTER COLUMN '
                           '"region" SET NOT NULL;',
                "derived_from": "region is NOT NULL and the source sends no "
                                "such column",
                "form": "drop_not_null"}
    state = _approval_state(services, thread, prepared)
    state["verdict"] = verdict(headline="Target rejects every row")
    try:
        nodes.request_approval(state, services, prepared, "relax_blocking_column")
    except nodes.AwaitingApproval:
        pass

    services.expire_approvals(
        now=time.time() + config.APPROVAL_TIMEOUT_SECONDS + 1)
    parent = notifier.final

    # It says plainly that it waited, how long, and that nothing happened.
    assert "waited 15 minutes" in parent, parent
    assert "Nobody answered" in parent, parent
    assert "changed nothing" in parent, parent
    assert "no backup was taken" in parent, parent
    assert "5 rows in public.target" in parent, parent

    # The offer is withdrawn, and the statement is still there to run.
    assert "approve" not in parent.lower(), parent
    assert "stays open" in parent, parent           # what happens next, truthfully
    assert "give up in" not in parent, parent
    assert "DROP NOT NULL" in parent, parent
    assert "SET NOT NULL" in parent, parent          # and how to undo it
    assert notifier.states[-1] == slack_io.NEEDS_HUMAN, notifier.states

    # An edited Slack message notifies nobody, so the person being waited
    # on is told once, in the thread, that the wait is over.
    assert any("15 minutes up" in r for r in notifier.replies), notifier.replies


def test_a_human_closing_the_incident_stops_an_approval():
    services = services_for("trial5_halt.json")
    supervisor = Supervisor(services, Registry(
        path=_SCRATCH / "reg-closed.json", namespace="sarah_moolai"))
    thread = services.notifier.post_incident("🔴 test")
    services.pending[thread] = {
        "app": TRIAL5, "thread": thread, "skill": "s", "fingerprint": "f",
        "plan": {}, "asked_at": time.time(), "state": "waiting",
        "question": "?", "summary": "DROP COLUMN a", "rows": 4, "backup": "b"}

    services.notifier.close_by_human(thread)
    answer = supervisor.approve(thread)

    assert "already marked resolved" in answer, answer
    assert services.pending[thread]["state"] == "waiting"  # never ran

    # ...and a new request against a closed incident is refused outright.
    try:
        nodes.request_approval(_approval_state(services, thread, {}), services,
                               {"statement": "x", "kind": ddl.DESTRUCTIVE},
                               "s")
    except target_repair.RepairRefused as error:
        assert "already marked this incident resolved" in str(error)
    else:
        raise AssertionError("it asked on a closed incident")


def test_approve_outside_a_thread_means_nothing():
    """The same word in the channel refers to no particular repair."""
    services = services_for("trial5_halt.json")
    console = slack_io.Console(
        Supervisor(services, Registry(path=_SCRATCH / "reg-ch.json",
                                      namespace="sarah_moolai")),
        bot_token=None, app_token=None)

    assert console.handle("approve") == slack_io.HELP or \
        console.handle("approve") is None
    assert "approve" in slack_io.HELP


def test_relaxing_a_constraint_runs_unasked_and_captures_nothing():
    """It removes no rows and no values, so it does not ask.

    It used to. On a pipeline where every write is being rejected, the
    choice is not "relax the constraint or don't", it is "relax the
    constraint or stay down" - and sarah1_CDC asked four times in one
    afternoon, was never answered, and sat TERMINATED throughout, while
    the message claimed it destroyed something. It destroys nothing.
    """
    schema = _Schema([("id", "integer", "NO"), ("region", "text", "NO")], [])

    with a_target(schema):
        prepared = ddl.plan(
            'ALTER TABLE "public"."target" ALTER COLUMN "region" DROP NOT NULL',
            derived_from="a measured schema diff", incident="i-nn")

    assert prepared["kind"] == ddl.ADDITIVE, prepared
    assert prepared["runnable"] is True
    assert prepared["backup_table"] is None, prepared   # nothing to copy

    # The way back is still written down, caveat and all.
    assert "SET NOT NULL" in prepared["restore"]
    assert "fails if any row is null by then" in prepared["restore"]


def test_only_removing_data_asks_for_approval():
    """The line between "runs" and "asks" is whether rows or values go."""
    schema = _Schema([("id", "integer", "NO"), ("region", "text", "NO")], [])

    with a_target(schema):
        asks = ddl.plan('ALTER TABLE "public"."target" DROP COLUMN "region"',
                        derived_from="measured", incident="i-1")["kind"]
        relaxes = ddl.plan('ALTER TABLE "public"."target" ALTER COLUMN '
                           '"region" DROP NOT NULL',
                           derived_from="measured", incident="i-2")["kind"]
        widens = ddl.plan('ALTER TABLE "public"."target" ALTER COLUMN '
                          '"region" TYPE text',
                          derived_from="measured", incident="i-3")["kind"]

    assert asks == ddl.DESTRUCTIVE, asks
    assert relaxes == ddl.ADDITIVE, relaxes
    assert widens == ddl.ADDITIVE, widens


# ======================================== THE REMAINING FAULT CLASSES
#
# The recurring bug across all of them: a check that succeeds read as
# "this end is healthy". Table exists != schema matches. Host answers !=
# credentials work. Status RUNNING != data moving.

def _catalog(columns, privileges=None, table="target"):
    """A target whose information_schema answers from a column spec."""
    granted = privileges if privileges is not None else {
        "SELECT": True, "INSERT": True, "UPDATE": True, "DELETE": True}

    class Cursor:
        description = None
        rowcount = -1
        _rows = []

        def execute(self, sql, params=()):
            low = " ".join(sql.lower().split())
            if "has_table_privilege" in low:
                self.description = [("p_select",), ("p_insert",),
                                    ("p_update",), ("p_delete",)]
                self._rows = [tuple(granted[p] for p in
                                    ("SELECT", "INSERT", "UPDATE", "DELETE"))]
                return self
            if "information_schema.columns" in low:
                side = params[1] if params else table
                self.description = [("column_name",), ("data_type",),
                                    ("is_nullable",), ("column_default",),
                                    ("is_identity",), ("identity_generation",),
                                    ("character_maximum_length",),
                                    ("numeric_precision",), ("numeric_scale",)]
                self._rows = [tuple(c) for c in columns.get(side, [])]
                return self
            if "constraint_type" in low:
                self.description = [("column_name",)]
                self._rows = [("id",)]
                return self
            if "information_schema.tables" in low:
                self.description = [("table_schema",), ("table_name",)]
                self._rows = [("public", params[0] if params else table)]
                return self
            self.description = [("?column?",)]
            self._rows = [(1,)]
            return self

        def fetchall(self):
            return self._rows

    class Handle:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    return Handle()


@contextlib.contextmanager
def a_catalog(columns, privileges=None):
    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL)
    db.CONNECT = lambda info, readonly=False: _catalog(columns, privileges)
    config.TARGET_DB_URL = "postgresql://target/db"
    config.SOURCE_DB_URL = "postgresql://source/db"
    try:
        yield
    finally:
        db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL = originals


# name, type, nullable, default, identity, generation, len, prec, scale
_ID = ("id", "integer", "NO", None, "NO", None, None, 32, 0)


def test_task5_4_a_revoked_privilege_is_found_not_eliminated():
    """The sharpest test in the set.

    The host answers, the table exists, the schema matches, db_probe
    succeeds - and every write fails. Everything the agent checks passes,
    so it concludes "the target is fine, must be the source". That is the
    elimination error, and this is the check that closes it.
    """
    columns = {"target": [_ID], "source": [_ID]}
    revoked = {"SELECT": True, "INSERT": False, "UPDATE": True, "DELETE": True}

    with a_catalog(columns, privileges=revoked):
        assert db.db_probe("target")["reachable"] is True      # host answers
        assert db.db_table_exists("target", "public.target")["exists"] is True
        assert db.db_compare_schema("public.source",
                                    "public.target")["matches"] is True

        grants = db.db_check_privileges("target", "public.target")

    assert grants["can_write"] is False, grants
    assert grants["missing"] == ["INSERT"]
    assert "cannot insert" in grants["note"]
    assert "do not go looking at the source" in grants["note"]

    # And the guardrail names permissions as the cause, before schema.
    reason = nodes.cause_still_present({
        "target_db": {"configured": True, "reachable": True},
        "target_table": {"table": "public.target", "exists": True},
        "target_privileges": grants})
    assert "no INSERT permission" in reason, reason
    assert "nothing about the host, the table or the schema will show it" \
        in reason


def test_task5_4_the_grant_is_written_out_exactly():
    statement = db.grant_statement("public.target", ["INSERT"], "neondb_owner")
    assert statement == ('GRANT INSERT ON "public"."target" TO '
                         '"neondb_owner";'), statement


def test_task5_4_privileges_that_could_not_be_read_are_not_a_pass():
    with a_catalog({"target": [_ID]}):
        original = db.CONNECT
        db.CONNECT = None                       # unreachable mid-check
        try:
            out = db.db_check_privileges("target", "public.target")
        finally:
            db.CONNECT = original
    assert out["configured"] is False or out.get("privileges") is None
    assert "fine" not in (out.get("note") or "").lower() or \
        "not 'the permissions are fine'" in out["note"]


def test_task5_1_a_type_mismatch_is_measured_not_read_from_an_error():
    """Blocked on exactly this: "couldn't check: exact declared length of
    target.name (shown only as varchar(3) in the error)". Reading a length
    out of an error string is the derivation the DDL rules forbid."""
    columns = {
        "source": [_ID, ("name", "text", "YES", None, "NO", None,
                         None, None, None)],
        "target": [_ID, ("name", "character varying", "YES", None, "NO", None,
                         3, None, None)],
    }

    with a_catalog(columns):
        described = db.db_describe_table("target", "public.target")
        diff = db.db_compare_schema("public.source", "public.target")

    # The length comes from information_schema, as a declared type.
    name = next(c for c in described["columns"] if c["column_name"] == "name")
    assert name["character_maximum_length"] == 3
    assert name["declared_type"] == "varchar(3)"

    # ...and the mismatch is first-class, not folded into missing/extra.
    assert diff["matches"] is False
    assert diff["missing_in_target"] == []
    mismatch = diff["differing_types"][0]
    assert mismatch == {"column": "name", "source_type": "text",
                        "target_type": "varchar(3)", "widening": True}
    assert "the target is NARROWER" in diff["note"], diff["note"]
    assert diff["repair"] == [
        'ALTER TABLE "public"."target" ALTER COLUMN "name" TYPE text;']


def test_task5_1_widening_executes_and_narrowing_captures_first():
    columns = {"target": [_ID, ("name", "character varying", "YES", None,
                                "NO", None, 3, None, None)]}

    with a_catalog(columns):
        widening = ddl.plan(
            'ALTER TABLE "public"."target" ALTER COLUMN "name" TYPE text',
            derived_from="a measured schema diff", incident="i-w")
        assert widening["kind"] == ddl.ADDITIVE, widening
        assert widening["runnable"] is True
        assert widening["backup_table"] is None      # nothing is lost

    columns = {"target": [_ID, ("name", "text", "YES", None, "NO", None,
                                None, None, None)]}
    with a_catalog(columns):
        narrowing = ddl.plan(
            'ALTER TABLE "public"."target" ALTER COLUMN "name" TYPE varchar(3)',
            derived_from="a measured schema diff", incident="i-n")
    assert narrowing["kind"] == ddl.DESTRUCTIVE, narrowing
    assert narrowing["backup_table"], narrowing


def test_task5_5_an_extra_not_null_column_the_source_cannot_supply():
    """The reverse direction. A source-to-target-only diff comes back
    clean and the agent is blind."""
    columns = {
        "source": [_ID],
        "target": [_ID, ("region", "text", "NO", None, "NO", None,
                         None, None, None)],
    }

    with a_catalog(columns):
        diff = db.db_compare_schema("public.source", "public.target")

    assert diff["matches"] is False, diff
    assert diff["missing_in_target"] == []          # nothing missing forward
    assert diff["extra_in_target"] == ["region"]

    blocking = diff["blocking_in_target"][0]
    assert blocking["column"] == "region"
    assert "NOT NULL with no default" in blocking["why"]
    assert "every write is rejected" in blocking["why"]

    # DROP NOT NULL is preferred over DROP COLUMN - it loses nothing.
    assert diff["repair"] == [
        'ALTER TABLE "public"."target" ALTER COLUMN "region" DROP NOT NULL;']
    assert not any("DROP COLUMN" in r for r in diff["repair"])


def test_task5_5_a_nullable_extra_column_is_not_a_fault():
    columns = {
        "source": [_ID],
        "target": [_ID, ("note", "text", "YES", None, "NO", None,
                         None, None, None)],
    }
    with a_catalog(columns):
        diff = db.db_compare_schema("public.source", "public.target")

    assert diff["extra_in_target"] == ["note"]
    assert diff["blocking_in_target"] == []
    assert diff["matches"] is True, diff["note"]


def test_task5_3_a_dropped_slot_names_itself_and_the_plugin():
    """Zone 3. Never write to the source, and never say "check the source
    system"."""
    skill = skills.SKILLS["escalate_source_problem"]
    state = {"app": "sarah_moolai.sarah1_CDC", "status": "HALT",
             "evidence": {
                 "source_config": {
                     "component": "ns.App_PostgresCDC",
                     "expects": "public.source",
                     "properties": {"replicationslotname": "striim_slot",
                                    "connectorclass": "wal2json"}},
                 "replication_slot": {"configured": True, "exists": False,
                                      "slots": []},
                 "source_endpoint": {"host": "ep-x.neon.tech", "port": 5432,
                                     "database": "neondb"},
                 "target_rows": {"rows": 1204}}}

    ok, why = skill.applies(state)
    assert ok, why
    plan = skill.plan(state)

    assert plan.executable is False
    assert "striim_slot" in plan.reason
    assert "ep-x.neon.tech:5432/neondb" in plan.reason
    assert "duplicate the 1,204 rows already in the target" in plan.reason

    statements = "\n".join(plan.statements)
    assert ("SELECT pg_create_logical_replication_slot('striim_slot', "
            "'wal2json');") in statements, statements
    assert "fresh initial load" in statements
    assert "run this on the source" in statements
    assert any("must be wal2json" in w for w in plan.warnings)

    # The agent writes nothing to the source, and says so.
    assert "never writes to" in plan.effect


def test_task5_3_the_plugin_is_read_from_the_reader_not_assumed():
    evidence = {"source_config": {"properties": {"connectorclass": "pgoutput"}}}
    assert skills._configured_plugin(evidence) == "pgoutput"

    unknown = {"source_config": {"properties": {"connectorclass": "banana"}}}
    assert skills._configured_plugin(unknown) is None


def test_task5_2_rejections_while_running_are_a_fault():
    """Finding §6 made actionable: rows dropped silently while the
    platform reports healthy."""
    assert "REJECTIONS WHILE RUNNING ARE A FAULT" in prompts.SYSTEM
    assert "NOT in the target" in prompts.SYSTEM

    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    out = st.get_rejected_events(striim, REJECTING)
    assert out["has_ever_rejected"] is True
    assert out["seconds_since_last_rejection"] is not None
    assert "238s ago" in out["note"]


def test_task5_7_the_recurring_bug_is_named_in_the_prompt():
    """Every face of it, in one place, because it keeps coming back."""
    assert "A CHECK THAT SUCCEEDS IS NOT A CLEAN BILL OF HEALTH" in prompts.SYSTEM
    for pair in ("its schema matches", "the credentials can write",
                 "data is moving", "the position advanced"):
        assert pair in prompts.SYSTEM, pair
    assert "A STALL IS A FAULT" in prompts.SYSTEM


# ============================================== SILENT STALL (TASK-5 §6)
#
# Observed live: sarah1_CDC reported RUNNING, moved nothing for 34 minutes,
# its replication slot was inactive, and ten consecutive checkpoints
# recorded the identical LSN 0/1D5F690 while their timestamps advanced.
# Every check the platform makes passed.

def _cdc_evidence(position="lsn:0/1D5F690", active=False, quiet=2100,
                  checkpoint="2026-08-08 11:42:48", pending=4_194_304,
                  server_lsn="0/2000000"):
    """CDC evidence.

    `server_lsn` is pg_current_wal_lsn() and `position` carries the slot's
    confirmed flush. Their MOVEMENT across observations is what separates
    stalled from idle - a single reading cannot, because an idle Postgres
    advances its WAL for checkpoints and autovacuum that no logical slot
    ever consumes.
    """
    return {
        "flow": {"source": {"component": "ns.App_PostgresCDC",
                            "seconds_since_last_event": quiet,
                            "profile": {"behaviour": "cdc_reader",
                                        "silence_is_normal": False,
                                        "why": "change capture on a live "
                                               "database"}},
                 "target": {"component": "ns.App_Target",
                            "seconds_since_last_write": quiet,
                            "profile": {"behaviour": "transactional_writer"}}},
        "replication_slot": {
            "configured": True, "exists": True,
            "inactive": ["striim_slot"] if not active else [],
            "pending_bytes": pending,
            "server_lsn": server_lsn,
            "confirmed_flush_lsn": position.split(":", 1)[-1],
            "slots": [{"slot_name": "striim_slot", "plugin": "wal2json",
                       "active": active, "pending_bytes": pending,
                       "server_lsn": server_lsn,
                       "confirmed_flush_lsn": position.split(":", 1)[-1]}]},
        "checkpoints": {"has_usable_checkpoint": True,
                        "last_checkpoint_time": checkpoint},
    }


def _watched(services, app, state, active=False, quiet=2100,
             position="lsn:0/1D5F690", source_moving=True):
    """Record STALL_OBSERVATIONS looks and return the last evidence.

    `source_moving` is the whole question: it advances pg_current_wal_lsn()
    between looks, which is the source producing work. With it False the
    server sits still too, which is an idle pipeline.
    """
    ev = None
    for n in range(config.STALL_OBSERVATIONS):
        ev = _cdc_evidence(
            position=position, active=active, quiet=quiet,
            checkpoint=f"2026-08-08 11:4{n}:00",
            server_lsn=f"0/2{n}00000" if source_moving else "0/2000000")
        nodes.observe(services, app, state, ev)
    return ev


def test_an_inactive_slot_while_running_is_a_stall():
    """The signal that needs no history - diagnosable on the first look."""
    services = services_for("trial5_running.json")
    state = {"app": "ns.App", "status": "RUNNING"}
    ev = _watched(services, "ns.App", state, active=False, source_moving=True)

    found = nodes.stall(state, services, ev)

    assert found["stalled"] is True, found
    assert any(s["signal"] == "slot_inactive" for s in found["signals"])
    assert "nothing is reading it" in found["note"]
    assert "reports this pipeline as healthy" in found["why_it_matters"]

    # An active slot keeping up with a moving source is not a stall.
    other = services_for("trial5_running.json")
    healthy = _watched(other, "ns.App2", state, active=True, quiet=5,
                       source_moving=True)
    healthy_slot = {**healthy}
    assert nodes.stall({"app": "ns.App2", "status": "RUNNING"}, other,
                       healthy_slot)["stalled"] is True    # slot still frozen

    # ...and once the slot follows the source, it is plainly fine.
    third = services_for("trial5_running.json")
    for n in range(config.STALL_OBSERVATIONS):
        moving = _cdc_evidence(position=f"lsn:0/1D5F6{n}0", active=True,
                               quiet=5, server_lsn=f"0/2{n}00000")
        nodes.observe(third, "ns.App3", state, moving)
    assert nodes.stall({"app": "ns.App3", "status": "RUNNING"}, third,
                       moving)["stalled"] is False


def test_a_position_that_does_not_move_while_checkpoints_do_is_a_stall():
    """Ten checkpoints at one LSN with advancing timestamps."""
    services = services_for("trial5_running.json")
    state = {"app": "ns.App", "status": "RUNNING"}

    # The slot is ACTIVE here, so only the frozen position can catch it.
    ev = _watched(services, "ns.App", state, active=True, quiet=5,
                  source_moving=True)

    found = nodes.stall(state, services, ev)
    assert found["stalled"] is True, found
    frozen = next(s for s in found["signals"]
                  if s["signal"] == "position_frozen")
    assert "0/1D5F690" in frozen["detail"]
    assert "checkpointing, not reading" in frozen["detail"]


def test_a_position_that_moves_is_not_a_stall():
    services = services_for("trial5_running.json")
    state = {"app": "ns.App", "status": "RUNNING"}

    for n in range(config.STALL_OBSERVATIONS + 1):
        ev = _cdc_evidence(position=f"lsn:0/1D5F6{n}0", active=True, quiet=5,
                           checkpoint=f"2026-08-08 11:4{n}:00",
                           server_lsn=f"0/2{n}00000")
        nodes.observe(services, "ns.App", state, ev)

    assert nodes.stall(state, services, ev)["stalled"] is False


def test_an_idle_pipeline_is_not_a_stall_and_raises_nothing():
    """The false incident, reproduced.

    Inactive slot, frozen LSN, zero throughput for eleven minutes - every
    symptom of a stall, shown by a pipeline whose source nobody was
    writing to. "Nothing moved" is not a fault until you have established
    there was something to move.
    """
    services = services_for("trial5_running.json")
    state = {"app": "ns.App", "status": "RUNNING"}

    # Slot inactive, position unchanged, source LSN also unchanged. Note
    # pending_bytes is NOT zero - an idle Postgres still advances its WAL
    # for checkpoints and autovacuum, which no logical slot consumes, so a
    # healthy quiet pipeline always looks a few megabytes "behind". Reading
    # that as a backlog is what raised the false incident on sarah1_CDC.
    idle = _watched(services, "ns.App", state, active=False, quiet=651,
                    source_moving=False)

    assert idle["replication_slot"]["pending_bytes"] > 0     # and still fine
    history = services.observations["ns.App"]
    assert nodes.work_outstanding(idle, history) is False
    assert nodes.stall(state, services, idle)["stalled"] is False

    # The identical picture, with the source actually producing, IS a stall.
    busy_services = services_for("trial5_running.json")
    busy = _watched(busy_services, "ns.App", state, active=False, quiet=651,
                    source_moving=True)
    assert nodes.stall(state, busy_services, busy)["stalled"] is True


def test_a_question_that_cannot_be_answered_is_not_a_fault():
    """No slot, no reconcile, nothing to go on - so no claim either way."""
    services = services_for("trial5_running.json")
    blind = {"flow": {"source": {"seconds_since_last_event": 9999,
                                 "profile": {"silence_is_normal": False,
                                             "why": "change capture"}},
                      "target": {"seconds_since_last_write": 9999}}}

    assert nodes.work_outstanding(blind, []) is None
    assert nodes.stall({"app": "ns.App", "status": "RUNNING"},
                       services, blind)["stalled"] is False


def test_a_calm_pipeline_with_nothing_measurably_wrong_stays_quiet():
    """Even when the model says otherwise. The code decides."""
    services = services_for("trial5_running.json")
    unhappy = verdict(healthy=False, fault_zone="source", confidence="medium",
                      headline="It has not moved anything in a while")

    out = nodes.decide(
        {"app": TRIAL5, "status": "RUNNING", "verdict": unhappy,
         "evidence": _cdc_evidence(active=False, quiet=651, pending=0)},
        services)

    assert out["tier"] is None, (out["tier"], out["reason"])
    assert out["silent"] is True
    assert "a quiet source is not a fault" in out["reason"]


def test_silence_that_is_normal_for_the_connector_is_not_a_stall():
    """A file reader that has finished is quiet and perfectly healthy."""
    services = services_for("trial5_running.json")
    ev = _cdc_evidence(active=True, quiet=99999)
    ev["flow"]["source"]["profile"] = {"behaviour": "file_reader",
                                       "silence_is_normal": True}

    found = nodes.stall({"app": "ns.App", "status": "RUNNING"}, services, ev)
    assert found["stalled"] is False, found


def test_a_stall_lets_wait_and_resume_fire():
    """It diagnosed this correctly and then declined: "RUNNING is not a
    fault state". A stalled CDC reader reattaching to its slot is the
    textbook safe restart."""
    skill = skills.SKILLS["wait_and_resume"]

    not_stalled = {"app": "ns.App", "status": "RUNNING",
                   "evidence": {**_cdc_evidence(active=True, quiet=5),
                                "stall": {"stalled": False}}}
    ok, why = skill.applies(not_stalled)
    assert ok is False
    assert "nothing suggests it has stalled" in why, why

    stalled = {"app": "ns.App", "status": "RUNNING",
               "evidence": {**_cdc_evidence(),
                            "stall": {"stalled": True,
                                      "note": "slot inactive"}}}
    ok, why = skill.applies(stalled)
    assert ok is True, why
    assert "reattaches it to the position its slot still holds" in why


def test_a_stall_is_repaired_in_the_pipeline_wherever_it_points():
    """Observed live, and it stopped a repair dead.

    sarah1_CDC stalled with an inactive replication slot. The diagnosis
    called that a SOURCE fault - a fair reading, the slot lives on the
    source - and the source-zone rule refused to act, so wait_and_resume
    was never consulted at all. But restarting our own reader is a
    pipeline action; a slot being on the source does not make it a write
    to somebody else's database.
    """
    services = services_for("trial5_running.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-stall.json"))
    evidence = {**_cdc_evidence(active=False),
                "stall": {"stalled": True, "note": "slot inactive"}}

    state = {"app": TRIAL5, "status": "RUNNING", "evidence": evidence,
             "verdict": verdict(healthy=False, fault_zone="source",
                                confidence="high", restart_would_help=True,
                                why_not="", human_action_needed="",
                                likely_cause="the slot went inactive",
                                suggested_skill="wait_and_resume")}

    out = nodes.decide(state, services)

    assert out["fault_zone"] == "pipeline", out["fault_zone"]
    assert out["tier"] == 1, (out["tier"], out["reason"])
    assert out["skill"] == "wait_and_resume", out.get("skill_dropped")


def test_a_stall_that_is_really_a_missing_slot_still_refuses():
    """Reclassifying the zone must not weaken anything downstream."""
    services = services_for("trial5_running.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-gone.json"))
    evidence = {**_cdc_evidence(active=False),
                "stall": {"stalled": True, "note": "slot inactive"},
                "replication_slot": {"configured": True, "exists": False,
                                     "slots": []}}

    out = nodes.decide(
        {"app": TRIAL5, "status": "RUNNING", "evidence": evidence,
         "verdict": verdict(healthy=False, fault_zone="source",
                            confidence="high", restart_would_help=True,
                            why_not="", suggested_skill="wait_and_resume")},
        services)

    assert out["tier"] == 3, (out["tier"], out["reason"])
    assert "no position to resume from" in out["reason"], out["reason"]


def test_a_named_cause_does_not_also_get_the_never_compared_warning():
    """It appeared under a stall incident that already named its cause,
    contradicting the finding printed next to it."""
    stalled = {"stall": {"stalled": True},
               "target_table": {"table": "public.target", "exists": True},
               "schema_diff": {"checked": False}}
    assert nodes._cause_is_named(stalled) is True

    # ...but a genuinely unexplained fault still gets the warning.
    unexplained = {"target_table": {"table": "public.target", "exists": True},
                   "schema_diff": {"checked": False},
                   "errors": {"error_count": 0}}
    assert nodes._cause_is_named(unexplained) is False
    assert nodes._schema_not_compared(unexplained)


def test_a_stalled_pipeline_is_not_described_as_stopped():
    """The headline contradicted the console: "stopped, not safe to
    restart" about something reading RUNNING."""
    text = slack_io.compose_incident(
        "sarah_moolai.sarah1_CDC",
        verdict(headline="Reading nothing while the slot sits inactive",
                what_happened="It is running and reading nothing."),
        {"shape": "stalled",
         "blocked_reason": "the replication slot is inactive",
         "next_action": "restart it to reattach the slot"})

    assert "Reading nothing while the slot sits inactive" in text, text
    assert "Not restarting: the replication slot is inactive" in text
    assert "stopped" not in text.lower(), text


def test_verification_reads_the_position_not_the_checkpoint_blob():
    """The bug this uncovered: a stalled pipeline writing its tenth
    checkpoint at the same LSN changes the checkpoint fingerprint every
    time, so the old check would have verified a stall as a recovery."""
    striim = st.Striim.offline(FIXTURES / "trial5_running.json")

    before = {"status": "RUNNING", "position": "lsn:0/1D5F690",
              "target_seconds_since_write": 10,
              "checkpoint_fingerprint": "old", "source_behaviour": "cdc_reader"}

    # Checkpoints keep being written - the fingerprint changes - but the
    # position does not move.
    frozen = act.verify(striim, TRIAL5, before, wait=0, verbose=False,
                        position_now=lambda: "lsn:0/1D5F690")
    assert frozen["recovered"] is False, frozen
    assert "has not moved" in frozen["detail"], frozen["detail"]

    advanced = act.verify(striim, TRIAL5, before, wait=0, verbose=False,
                          position_now=lambda: "lsn:0/1D6A000")
    assert advanced["recovered"] is True, advanced
    assert "advanced from lsn:0/1D5F690" in advanced["detail"]


def test_the_position_comes_from_the_slot_before_the_component():
    """The slot is the authority for change capture."""
    assert nodes.position_of(_cdc_evidence()) == "lsn:0/1D5F690"

    no_slot = {"source_config": {"read_position": {"offset_end_bytes": 4096}}}
    assert nodes.position_of(no_slot) == "offset:4096"
    assert nodes.position_of({}) is None


# ==================================================== BACKFILL
#
# A restored column is NULL for every row that existed before it was
# dropped - those values went with the column, and change capture only
# delivers new changes. Schema right, data wrong, pipeline green.

class _TwoDatabases:
    """A source and a target that are genuinely separate connections.

    Not one database with a join available - that is the assumption the
    backfill must never make, since the two ends can be different servers.
    """

    def __init__(self, source_rows, target_rows, key="id", column="age",
                 key_type="integer", column_type="integer"):
        self.source = dict(source_rows)          # id -> value or None
        self.target = dict(target_rows)
        self.key, self.column = key, column
        self.key_type, self.column_type = key_type, column_type
        self.statements = []

    def handle(self, readonly):
        outer = self

        class Cursor:
            description = None
            rowcount = -1
            _rows = []

            def execute(self, sql, params=()):
                outer.statements.append((sql, readonly))
                low = " ".join(sql.lower().split())
                params = params or ()
                side = outer.source if readonly else outer.target

                if "information_schema.columns" in low:
                    self.description = [("column_name",), ("data_type",),
                                        ("is_nullable",), ("column_default",),
                                        ("is_identity",),
                                        ("identity_generation",)]
                    self._rows = [
                        (outer.key, outer.key_type, "NO", None, "NO", None),
                        (outer.column, outer.column_type, "YES", None,
                         "NO", None)]
                    return self
                if "constraint_type" in low:
                    self.description = [("column_name",)]
                    self._rows = [(outer.key,)]
                    return self

                if low.startswith("select count(*)"):
                    if "is not null" in low:
                        n = sum(1 for v in side.values() if v is not None)
                    elif "is null" in low:
                        n = sum(1 for v in side.values() if v is None)
                    else:
                        n = len(side)               # every row
                    self.description = [("count",)]
                    self._rows = [(n,)]
                    return self

                # Which keys does this side already hold?
                if low.startswith("select") and "= any(" in low:
                    wanted = params[0]
                    if " as k, " in low or low.count(" as ") > 1:
                        # source lookup for the column backfill: key + value
                        self.description = [("k",), ("v",)]
                        self._rows = [(k, side[k]) for k in wanted
                                      if side.get(k) is not None]
                    else:
                        self.description = [("k",)]
                        self._rows = [(k,) for k in wanted if k in side]
                    return self

                if low.startswith("select") and " limit " in low:
                    after = params[0] if params else None
                    limit = int(low.rsplit("limit", 1)[1].strip())
                    nulls_only = "is null" in low

                    keys = sorted(
                        k for k, v in side.items()
                        if (not nulls_only or v is None)
                        and (after is None or k > after))[:limit]

                    if " as k" in low:              # keys only
                        self.description = [("k",)]
                        self._rows = [(k,) for k in keys]
                    else:                           # whole rows
                        self.description = [(outer.key,), (outer.column,)]
                        self._rows = [(k, side[k]) for k in keys]
                    return self

                if low.startswith("insert into"):
                    assert not readonly, "a write reached the source"
                    assert "on conflict do nothing" in low, sql
                    added = 0
                    for k, v in zip(params[::2], params[1::2]):
                        if k not in outer.target:   # never overwrite
                            outer.target[k] = v
                            added += 1
                    self.rowcount = added
                    self.description = None
                    return self

                if low.startswith("update"):
                    assert not readonly, "a write reached the source"
                    changed = 0
                    for k, v in zip(params[::2], params[1::2]):
                        if k in outer.target and outer.target[k] is None:
                            outer.target[k] = v
                            changed += 1
                    self.rowcount = changed
                    self.description = None
                    return self

                self.description = [("?column?",)]
                self._rows = [(1,)]
                return self

            def fetchall(self):
                return self._rows

        class Handle:
            def cursor(self_inner):
                return Cursor()

            def commit(self_inner):
                pass

            def rollback(self_inner):
                pass

            def close(self_inner):
                pass

        return Handle()


@contextlib.contextmanager
def two_databases(source_rows, target_rows, **kwargs):
    pair = _TwoDatabases(source_rows, target_rows, **kwargs)
    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL)
    db.CONNECT = lambda info, readonly=False: pair.handle(readonly)
    config.TARGET_DB_URL = "postgresql://target-host/db"
    config.SOURCE_DB_URL = "postgresql://source-host/db"
    try:
        yield pair
    finally:
        db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL = originals


def test_a_restored_column_is_filled_in_from_the_source():
    """The gap: schema fixed, every pre-existing row still empty."""
    source = {i: i + 20 for i in range(1, 51)}
    target = {i: None for i in range(1, 51)}

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target", "age",
                              incident="i-1", batch_rows=10)
        assert found["runnable"] is True, found["reason"]
        assert found["key"] == "id"
        assert found["nulls_in_target"] == 50
        assert found["source_has_values"] == 50

        result = backfill.run(found)

    assert result["rows_filled"] == 50, result
    assert result["nulls_after"] == 0
    assert "no row is left with a null age" in result["note"]
    assert pair.target[7] == 27

    # It batched rather than sending one enormous statement.
    updates = [s for s, _ in pair.statements if s.lower().startswith("update")]
    assert len(updates) == 5, len(updates)


def test_a_recreated_table_is_backfilled_with_the_rows_it_never_saw():
    """The drop-table case, and it is worse than the dropped column.

    There the rows existed with one value missing. Here they do not exist
    at all - and change capture carries changes forward only, so a table
    recreated today holds nothing that happened before today. Recreating
    the structure and stopping leaves a pipeline that reports healthy over
    an empty table.
    """
    source = {i: i + 20 for i in range(1, 1205)}          # 1,204 rows
    target = {}                                            # just recreated

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target",
                              incident="i-drop", batch_rows=500)

        assert found["scope"] == backfill.ROWS, found
        assert found["runnable"] is True, found["reason"]
        assert found["key"] == "id"
        assert found["source_rows"] == 1204
        assert found["target_rows"] == 0
        assert found["keys_missing"] == 1204               # measured, exact
        assert "missing 1,204 row(s)" in found["reason"], found["reason"]

        result = backfill.run(found)

    assert result["rows_filled"] == 1204, result
    assert result["keys_missing_after"] == 0
    assert result["target_rows_after"] == 1204
    assert "both hold every key" in result["note"], result["note"]
    assert pair.target[1] == 21 and pair.target[1204] == 1224

    inserts = [s for s, _ in pair.statements
               if s.lower().startswith("insert")]
    assert len(inserts) == 3, len(inserts)                 # 500/500/204
    assert "ON CONFLICT DO NOTHING" in inserts[0], inserts[0]


def test_a_row_the_target_already_holds_is_never_re_inserted():
    """Only keys the target lacks. Two guarantees, both tested."""
    source = {1: 21, 2: 22, 3: 23}
    target = {2: 999}                       # already there, and different

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target")
        assert found["keys_missing"] == 2, found
        result = backfill.run(found)

    assert result["rows_filled"] == 2
    assert pair.target[2] == 999, "an existing row was overwritten"
    assert pair.target == {1: 21, 2: 999, 3: 23}

    # ...and the statement itself carries the second guarantee, so a row
    # arriving between the check and the insert is still left alone.
    for statement, _ in pair.statements:
        if statement.lower().startswith("insert"):
            assert "ON CONFLICT DO NOTHING" in statement


def test_rows_deleted_at_source_are_not_reported_as_a_gap():
    """The target holding MORE than the source is not something missing."""
    source = {1: 21}
    target = {1: 21, 2: 22, 3: 23}          # 2 and 3 deleted at source

    with two_databases(source, target):
        found = backfill.plan("public.source", "public.target")

    assert found["runnable"] is False
    assert found["keys_missing"] == 0, found
    assert "already holds every key" in found["reason"], found["reason"]


def test_a_remaining_gap_is_counted_and_explained():
    """Never "backfilled" without the number, and name the likely reason."""
    source = {1: 21, 2: 22}
    target = {}

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target")

        # A row vanishes at source mid-run, as one genuinely can.
        original = backfill._count_missing_keys

        def after_the_fact(f):
            if f.get("target_rows"):        # the verify pass
                return 1
            return original(f)

        backfill._count_missing_keys = after_the_fact
        try:
            result = backfill.run(found)
        finally:
            backfill._count_missing_keys = original

    assert "still missing 1 key(s)" in result["note"], result["note"]
    assert "deleted at source" in result["note"]
    assert str(result["rows_filled"]) in result["note"]


def test_the_row_backfill_never_writes_to_the_source():
    with two_databases({1: 21, 2: 22}, {}) as pair:
        backfill.run(backfill.plan("public.source", "public.target"))

    for statement, readonly in pair.statements:
        if readonly:
            assert statement.strip().lower().startswith(("select", "with")), \
                statement


def test_the_preview_covers_both_scopes_and_writes_nothing():
    with two_databases({1: 21, 2: 22}, {}) as pair:
        rows = db.TOOLS["db_backfill_preview"]("public.source",
                                               "public.target")
    assert rows["scope"] == backfill.ROWS
    assert rows["rows_missing_from_target"] == 2
    assert rows["rows_in_source"] == 2
    assert not [s for s, _ in pair.statements
                if s.lower().startswith(("insert", "update"))]

    with two_databases({1: 21}, {1: None}) as pair:
        column = db.TOOLS["db_backfill_preview"]("public.source",
                                                 "public.target", "age")
    assert column["scope"] == backfill.COLUMN
    assert column["rows_with_null_in_target"] == 1


def test_no_shared_key_declines_a_row_backfill_too():
    with two_databases({1: 21}, {}):
        original = db.db_describe_table
        db.db_describe_table = lambda which, table: {
            **original(which, table),
            "primary_key": [] if which == "target" else ["id"]}
        try:
            found = backfill.plan("public.source", "public.target")
        finally:
            db.db_describe_table = original

    assert found["runnable"] is False
    assert "no primary key" in found["reason"], found["reason"]
    assert "rows need filling in by hand" not in found["reason"]


def test_a_null_that_is_genuinely_null_at_source_stays_null_and_is_counted():
    """Never report 'backfilled' without the number, and never round the
    remainder away."""
    source = {1: 21, 2: None, 3: 23, 4: None}
    target = {1: None, 2: None, 3: None, 4: None}

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target", "age")
        result = backfill.run(found)

    assert result["rows_filled"] == 2, result
    assert result["nulls_after"] == 2
    assert "2 row(s) still have a null age" in result["note"], result["note"]
    assert "genuinely null rather than missed" in result["note"]
    assert pair.target == {1: 21, 2: None, 3: 23, 4: None}


def test_a_value_the_target_already_holds_is_never_overwritten():
    """WHERE target.col IS NULL, on every statement."""
    source = {1: 99, 2: 88}
    target = {1: 21, 2: None}          # row 1 already has a value

    with two_databases(source, target) as pair:
        found = backfill.plan("public.source", "public.target", "age")
        result = backfill.run(found)

    assert pair.target[1] == 21, "an existing value was overwritten"
    assert pair.target[2] == 88
    assert result["rows_filled"] == 1

    for statement, _ in pair.statements:
        if statement.lower().startswith("update"):
            assert '"age" IS NULL' in statement, statement


def test_the_backfill_never_writes_to_the_source():
    source = {1: 21}
    target = {1: None}
    with two_databases(source, target) as pair:
        backfill.run(backfill.plan("public.source", "public.target", "age"))

    for statement, readonly in pair.statements:
        if readonly:
            assert statement.strip().lower().startswith(("select", "with")), \
                statement


def test_no_shared_primary_key_means_no_backfill():
    """No key, no safe way to match rows."""
    source = {1: 21}
    target = {1: None}

    with two_databases(source, target) as pair:
        original = db.db_describe_table

        def no_key(which, table):
            out = original(which, table)
            if which == "target":
                out["primary_key"] = []
            return out

        db.db_describe_table = no_key
        try:
            found = backfill.plan("public.source", "public.target", "age")
        finally:
            db.db_describe_table = original

    assert found["runnable"] is False
    assert "no primary key" in found["reason"], found["reason"]
    assert "no safe way to match" in found["reason"]

    try:
        backfill.run(found)
    except backfill.BackfillRefused:
        pass
    else:
        raise AssertionError("a backfill ran with no primary key")


def test_mismatched_keys_and_composite_keys_are_declined():
    source, target = {1: 21}, {1: None}
    original = db.db_describe_table

    with two_databases(source, target):
        def differing(which, table):
            out = original(which, table)
            out["primary_key"] = ["id"] if which == "source" else ["uuid"]
            return out

        db.db_describe_table = differing
        try:
            found = backfill.plan("public.source", "public.target", "age")
        finally:
            db.db_describe_table = original
        assert found["runnable"] is False
        assert "primary keys differ" in found["reason"], found["reason"]

        def composite(which, table):
            out = original(which, table)
            out["primary_key"] = ["tenant", "id"]
            return out

        db.db_describe_table = composite
        try:
            found = backfill.plan("public.source", "public.target", "age")
        finally:
            db.db_describe_table = original
        assert found["runnable"] is False
        assert "composite" in found["reason"], found["reason"]
        assert "by hand" in found["reason"]


def test_nothing_to_fill_in_is_not_a_failure():
    with two_databases({1: 21}, {1: 21}):
        found = backfill.plan("public.source", "public.target", "age")
    assert found["runnable"] is False
    assert "nothing to fill in" in found["reason"], found["reason"]


def test_the_preview_writes_nothing():
    with two_databases({1: 21, 2: 22}, {1: None, 2: None}) as pair:
        out = db.TOOLS["db_backfill_preview"](
            "public.source", "public.target", "age")

    assert out["would_run"] is True
    assert out["rows_with_null_in_target"] == 2
    assert out["rows_with_a_value_in_source"] == 2
    assert out["matched_on"] == "id"
    assert not [s for s, _ in pair.statements
                if s.lower().startswith("update")]


def test_the_ddl_repair_and_the_backfill_read_as_one_repair():
    """The incident says what was done, in order, with numbers."""
    record = {"shape": "fixed",
              "did": ["added age (integer)",
                      "backfilled 1,204 rows from the source",
                      "resumed the pipeline"],
              "result": "0 rows left with a null age, checkpoint advancing"}
    text = slack_io.compose_incident(
        "sarah_moolai.Traill",
        verdict(what_happened="The target was missing the age column, so "
                              "every write failed."),
        record)

    assert "— fixed" in text
    assert ("I did:  added age (integer), then backfilled 1,204 rows from "
            "the source, then resumed the pipeline") in text, text
    assert "Result: 0 rows left with a null age" in text
    assert "Run this on" not in text


# ===================================================== DDL, THREE CLASSES
#
# The target is a replica, so its shape is repairable and every destructive
# statement can be made recoverable by copying what it removes first. That
# is the requirement, not a gate - which means the tests are about whether
# the copy exists and holds the rows, not about whether the statement ran.

class _Schema:
    """A target database with a real table, for DDL to act on."""

    def __init__(self, columns, rows, table="public.target", key="id"):
        self.columns = list(columns)          # [(name, type, nullable)]
        self.rows = list(rows)                # list of dicts
        self.table = table
        self.key = key
        # The source side reads the same shape - a schema is a schema, and
        # create_table_from_source needs one to copy.
        self.tables = {table, "public.source"}
        self.created = {}                     # backup name -> rows
        self.statements = []

    def handle(self, readonly):
        outer = self

        class Cursor:
            description = None
            rowcount = -1
            _rows = []

            def execute(self, sql, params=()):
                outer.statements.append(sql)
                low = " ".join(sql.lower().split())

                if "information_schema.columns" in low:
                    wanted = params[1] if params else ""
                    self.description = [("column_name",), ("data_type",),
                                        ("is_nullable",), ("column_default",),
                                        ("is_identity",),
                                        ("identity_generation",)]
                    known = (outer.columns
                             if f"public.{wanted}" in outer.tables else [])
                    self._rows = [(n, t, nl, None, "NO", None)
                                  for n, t, nl in known]
                    return self
                if "information_schema.tables" in low:
                    wanted = params[0] if params else ""
                    self.description = [("table_schema",), ("table_name",)]
                    self._rows = ([("public", wanted)]
                                  if f"public.{wanted}" in outer.tables else [])
                    return self
                if "constraint_type" in low:
                    self.description = [("column_name",)]
                    self._rows = [(outer.key,)]
                    return self
                if low.startswith("create table") and " as select" in low:
                    name = sql.split()[2]
                    outer.created[name] = list(outer.rows)
                    outer.tables.add(name.replace('"', ""))
                    self.rowcount = len(outer.rows)
                    self.description = None
                    return self
                if low.startswith("select count(*)"):
                    self.description = [("count",)]
                    source = sql.split("FROM")[1].strip().split()[0]
                    held = outer.created.get(source)
                    self._rows = [(len(held if held is not None
                                       else outer.rows),)]
                    return self
                if low.startswith("alter table") or low.startswith("drop table"):
                    if "drop column" in low:
                        name = sql.split("DROP COLUMN")[1].strip().strip('"; ')
                        outer.columns = [c for c in outer.columns
                                         if c[0] != name]
                    elif "add column" in low:
                        parts = sql.split("ADD COLUMN")[1].strip().rstrip(";")
                        name = parts.split()[0].strip('"')
                        kind = " ".join(parts.split()[1:])
                        outer.columns.append((name, kind, "YES"))
                    elif low.startswith("drop table"):
                        outer.tables.discard(outer.table)
                    self.rowcount = -1
                    self.description = None
                    return self

                self.description = [("?column?",)]
                self._rows = [(1,)]
                return self

            def fetchall(self):
                return self._rows

        class Handle:
            def cursor(self_inner):
                return Cursor()

            def commit(self_inner):
                pass

            def rollback(self_inner):
                pass

            def close(self_inner):
                pass

        return Handle()


@contextlib.contextmanager
def a_target(schema):
    originals = (db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL)
    db.CONNECT = lambda info, readonly=False: schema.handle(readonly)
    config.TARGET_DB_URL = "postgresql://target/db"
    config.SOURCE_DB_URL = "postgresql://source/db"
    try:
        yield schema
    finally:
        db.CONNECT, config.TARGET_DB_URL, config.SOURCE_DB_URL = originals


def test_ddl_class_one_additive_executes_directly():
    """Nothing is lost, so nothing needs capturing first."""
    schema = _Schema([("id", "integer", "NO")], [{"id": 1}, {"id": 2}])

    with a_target(schema):
        prepared = ddl.plan(
            'ALTER TABLE "public"."target" ADD COLUMN "age" integer',
            derived_from="a measured schema diff", incident="i-1")
        assert prepared["kind"] == ddl.ADDITIVE, prepared
        assert prepared["runnable"] is True
        assert prepared["backup_table"] is None      # nothing to capture

        result = ddl.execute(prepared)

    assert result["verified"]["ok"] is True, result["verified"]
    assert "age" in [c[0] for c in schema.columns]
    assert not schema.created, "an additive change took a backup"


def test_ddl_class_one_type_widening_is_additive_narrowing_is_not():
    assert ddl.widens("integer", "bigint") is True
    assert ddl.widens("varchar(50)", "varchar(255)") is True
    assert ddl.widens("varchar(50)", "text") is True

    assert ddl.widens("bigint", "integer") is False
    assert ddl.widens("varchar(255)", "varchar(50)") is False
    assert ddl.widens("text", "varchar(50)") is False

    # Cannot tell must behave as narrowing - a redundant backup costs a few
    # megabytes, the other mistake costs the column.
    assert ddl.widens("jsonb", "tsvector") is None

    schema = _Schema([("id", "integer", "NO"), ("age", "bigint", "YES")],
                     [{"id": 1, "age": 5}])
    with a_target(schema):
        narrowing = ddl.plan(
            'ALTER TABLE "public"."target" ALTER COLUMN "age" TYPE integer',
            derived_from="a measured schema diff", incident="i-2")
    assert narrowing["kind"] == ddl.DESTRUCTIVE, narrowing
    assert narrowing["backup_table"], narrowing


def test_ddl_class_two_destructive_captures_before_it_runs():
    """The requirement: a named, queryable copy, and the way back."""
    rows = [{"id": n, "age": n + 20} for n in range(1, 5)]
    schema = _Schema([("id", "integer", "NO"), ("age", "integer", "YES")], rows)

    with a_target(schema):
        prepared = ddl.plan(
            'ALTER TABLE "public"."target" DROP COLUMN "age"',
            derived_from="a measured schema diff", incident="sarah#a1b2c3")

        assert prepared["kind"] == ddl.DESTRUCTIVE
        assert prepared["runnable"] is True
        assert prepared["counts"]["rows"] == 4
        assert prepared["counts"]["non_null_age"] == 4

        backup = prepared["backup_table"]
        assert backup.startswith("striim_agent_backup_")
        assert "sarah_a1b2c3" in backup
        assert len(backup) <= 63, backup           # Postgres identifier limit

        # The restore statement exists BEFORE anything is destroyed.
        assert "ADD COLUMN" in prepared["restore"]
        assert backup in prepared["restore"]
        assert '"id" = b."id"' in prepared["restore"], prepared["restore"]

        result = ddl.execute(prepared)

    # The copy was taken, first, and it holds the rows.
    order = [s for s in schema.statements
             if s.lower().startswith(("create table", "alter table"))]
    assert order[0].lower().startswith("create table"), order
    assert schema.created[db.qualify(backup)] == rows
    assert prepared["counts"]["backed_up"] == 4

    # And the column is really gone.
    assert result["verified"]["ok"] is True
    assert "age" not in [c[0] for c in schema.columns]


def test_ddl_class_two_will_not_destroy_what_it_cannot_match_back():
    """No primary key, no way to restore, so nothing is dropped."""
    schema = _Schema([("age", "integer", "YES")], [{"age": 1}], key=None)

    with a_target(schema):
        original = db.db_describe_table
        db.db_describe_table = lambda which, table: {
            **original(which, table), "primary_key": []}
        try:
            prepared = ddl.plan(
                'ALTER TABLE "public"."target" DROP COLUMN "age"',
                derived_from="a measured schema diff", incident="i-3")
        finally:
            db.db_describe_table = original

    assert prepared["runnable"] is False
    assert "no primary key" in prepared["reason"], prepared["reason"]
    assert "could not be matched back" in prepared["reason"]


def test_ddl_class_three_rename_checks_the_pipeline_first():
    """A rename the writer does not know about breaks a second thing."""
    schema = _Schema([("id", "integer", "NO"), ("age", "integer", "YES")], [])

    with a_target(schema):
        blocked = ddl.plan(
            'ALTER TABLE "public"."target" RENAME COLUMN "age" TO "years"',
            derived_from="a measured schema diff", incident="i-4",
            evidence={"target_config": {"properties":
                                        {"columnmap": "age=age"}}})
        assert blocked["kind"] == ddl.RENAME
        assert blocked["runnable"] is False
        assert "still referenced by" in blocked["reason"], blocked["reason"]

        free = ddl.plan(
            'ALTER TABLE "public"."target" RENAME COLUMN "age" TO "years"',
            derived_from="a measured schema diff", incident="i-4",
            evidence={"target_config": {"properties": {"tables": "public.target"}}})
        assert free["runnable"] is True, free["reason"]


def test_ddl_the_four_things_that_stay_refused():
    """And only these four."""
    schema = _Schema([("id", "integer", "NO")], [])

    with a_target(schema):
        for statement, marker in (
                ("DROP DATABASE neondb", "never run by this agent"),
                ("DROP SCHEMA public", "never run by this agent"),
                ('DROP TABLE "public"."target" CASCADE', "CASCADE"),
                ('ALTER TABLE "public"."t" DROP COLUMN "a"; DROP TABLE "x"',
                 "one object at a time")):
            found = ddl.plan(statement, derived_from="measured", incident="i")
            assert found["runnable"] is False, statement
            assert found["kind"] == ddl.REFUSED, statement
            assert marker in found["reason"], (statement, found["reason"])

        # The quiet one: no provenance means it was not measured.
        guessed = ddl.plan('ALTER TABLE "public"."target" DROP COLUMN "age"',
                           derived_from=None, incident="i")
        assert guessed["runnable"] is False
        assert "no recorded provenance" in guessed["reason"], guessed["reason"]
        assert "guess with correct syntax" in guessed["reason"]

        # And execute() refuses a plan that was not approved.
        try:
            ddl.execute({"runnable": False, "reason": "nope"})
        except ddl.DDLRefused as error:
            assert "nope" in str(error)
        else:
            raise AssertionError("execute() ran an unapproved plan")


def test_ddl_a_create_table_is_built_from_the_source_schema():
    """Not a template - every column comes from db_describe_table."""
    schema = _Schema([("id", "integer", "NO"), ("name", "text", "YES")], [])

    with a_target(schema):
        statement, provenance = ddl.create_table_from_source(
            "public.source", "public.target")

    assert statement == ('CREATE TABLE "public"."target" ("id" integer NOT '
                         'NULL, "name" text, PRIMARY KEY ("id"))'), statement
    assert "source schema of public.source" in provenance
    assert "2 columns" in provenance


def test_ddl_a_violated_constraint_is_reported_not_attempted():
    schema = _Schema([("id", "integer", "NO")], [{"id": 1}, {"id": 1}])

    with a_target(schema):
        original = ddl._violations
        ddl._violations = lambda found: 2
        try:
            found = ddl.plan(
                'ALTER TABLE "public"."target" ADD CONSTRAINT "u" UNIQUE ("id")',
                derived_from="a measured schema diff", incident="i-5")
        finally:
            ddl._violations = original

    assert found["runnable"] is False
    assert "2 row(s) already violate" in found["reason"], found["reason"]


def test_ddl_the_backup_and_restore_reach_the_thread_before_it_runs():
    rows = [{"id": 1, "age": 21}]
    schema = _Schema([("id", "integer", "NO"), ("age", "integer", "YES")], rows)
    notifier = Recorder()

    class Services:
        pass

    services = Services()
    services.notifier = notifier

    with a_target(schema):
        prepared = ddl.plan(
            'ALTER TABLE "public"."target" DROP COLUMN "age"',
            derived_from="a measured schema diff", incident="i-6")
        handle = notifier.post_incident("🔴 test")
        ddl.execute(prepared, services=services, thread=handle)

    announced = "\n".join(notifier.replies)
    assert "DROP COLUMN" in announced
    assert prepared["backup_table"] in announced
    assert "to undo:" in announced
    assert "1 rows in public.target" in announced
    assert "1 of them have a value for age" in announced
    assert "derived from a measured schema diff" in announced


def test_what_the_model_finds_reaches_the_guardrails():
    """The contradiction: the verdict quoted a schema diff the skill could
    not see, because the MODEL had called db_compare_schema and the result
    went into the conversation and nowhere else."""
    assert nodes._evidence_key("db_compare_schema", {}) == "schema_diff"
    assert nodes._evidence_key("db_probe", {"which": "source"}) == "source_db"
    assert nodes._evidence_key("db_probe", {"which": "target"}) == "target_db"
    assert nodes._evidence_key("db_table_exists",
                               {"which": "target"}) == "target_table"
    assert nodes._evidence_key("list_apps", {}) is None

    merged = nodes._merged({"evidence": {"a": 1}}, {"schema_diff": {"b": 2}})
    assert merged["evidence"] == {"a": 1, "schema_diff": {"b": 2}}
    assert nodes._merged({"evidence": {"a": 1}}, {}) == {}


def test_a_declined_skill_is_not_printed_twice():
    """It was both the incident's reason and a reply underneath it."""
    dropped = ("considered dedupe_target_rows, but this target appends and "
               "has no keys")
    services = services_for("trial5_halt.json")
    notifier = services.notifier

    nodes.escalate({"app": TRIAL5, "tier": 2, "reason": dropped,
                    "skill_dropped": dropped, "verdict": verdict(),
                    "evidence": {}}, services)

    assert notifier.replies.count(f"_{dropped}_") == 0, notifier.replies
    assert "has no keys" in notifier.posts[0], notifier.posts[0]

    # ...and when the reason is something else entirely, the dropped skill
    # still does not get a reply. "Considered X but declined" is not a
    # state change - it is the agent explaining itself, which belongs in
    # the terminal and the audit log, not in a thread somebody has to read.
    services = services_for("trial5_halt.json")
    nodes.escalate({"app": TRIAL5, "tier": 2, "reason": "something else",
                    "skill_dropped": dropped, "verdict": verdict(),
                    "evidence": {}}, services)
    assert services.notifier.replies == [], services.notifier.replies


def test_a_missing_column_blocks_a_restart_and_names_itself():
    reason = nodes.cause_still_present({
        "target_db": {"configured": True, "reachable": True},
        "target_table": {"table": "public.target", "exists": True},
        "schema_diff": {"missing_in_target": ["city"], "matches": False}})

    assert reason and "city" in reason, reason
    assert "the table exists but cannot accept" in reason


def test_a_blocking_column_blocks_a_restart_and_names_itself():
    """Observed live on sarah1_CDC. The agent named region as the column
    rejecting every write, restarted into it, and reported that the restart
    did not hold - because this gate only ever looked at what was MISSING
    from the target, never at what the target had that the source lacks."""
    reason = nodes.cause_still_present({
        "target_db": {"configured": True, "reachable": True},
        "target_table": {"table": "public.target", "exists": True},
        "schema_diff": {"missing_in_target": [], "matches": False,
                        "blocking_in_target": [
                            {"column": "region", "type": "text",
                             "why": "NOT NULL with no default"}]}})

    assert reason and "region" in reason, reason
    assert "restarting cannot change that" in reason, reason


def test_a_column_too_narrow_blocks_a_restart():
    reason = nodes.cause_still_present({
        "target_db": {"configured": True, "reachable": True},
        "target_table": {"table": "public.target", "exists": True},
        "schema_diff": {"missing_in_target": [], "matches": False,
                        "differing_types": [
                            {"column": "name", "source_type": "text",
                             "target_type": "varchar(3)", "widening": True}]}})

    assert reason and "name" in reason, reason
    assert "varchar(3)" in reason and "text" in reason, reason


def test_nothing_is_restarted_when_nothing_was_repaired():
    """THE STRUCTURAL HOLE. _run_repair returns "nothing to repair" for
    every path with no skill and no executable statement, and ACT fell
    straight through and restarted anyway. This is the last gate."""
    # Its own ledger: the second half really does reach the action layer,
    # and a RESUME against the shared one spends another test's budget.
    services = services_for("trial5_halt.json",
                            ledger=act.Ledger(path=_SCRATCH / "l-last-gate.json"))
    sent = []
    services.striim.post = lambda *a, **k: sent.append(a)   # would be the RESUME

    evidence = {"target_db": {"configured": True, "reachable": True},
                "target_table": {"table": "public.target", "exists": True},
                "schema_diff": {"matches": False, "missing_in_target": [],
                                "blocking_in_target": [{"column": "region"}]}}

    did = []
    result, refusal = nodes._run_resume(
        {"app": TRIAL5, "evidence": evidence, "slack_thread_ts": None},
        services, did, repaired=False)

    assert result is None, result
    assert refusal and "not restarting" in refusal and "region" in refusal, refusal
    assert did == [], did
    assert sent == [], sent

    # ...but a repair that just ran IS allowed through: it changed the very
    # thing that evidence describes, so the evidence is stale by definition.
    _, refused_after_repair = nodes._run_resume(
        {"app": TRIAL5, "evidence": evidence, "slack_thread_ts": None},
        services, [], repaired=True)
    assert refused_after_repair is None, refused_after_repair


def test_an_unreadable_side_is_not_reported_as_a_match():
    original = db.CONNECT
    db.CONNECT = None
    try:
        out = db.db_compare_schema("public.source", "public.target")
    finally:
        db.CONNECT = original
    assert out["comparable"] is False
    assert out["checked"] is False
    assert "NOT 'they match'" in out["note"], out["note"]


def test_the_banner_does_not_claim_a_database_is_unconfigured():
    """It said 'not configured' while db_probe was succeeding against it."""
    summary = config.summary()
    assert summary["target_db"] == "discovered per pipeline"
    assert "read-only" in summary["source_db"]
    for value in summary.values():
        assert "not configured" not in str(value), summary


def test_db_probe_answers_three_ways_never_two():
    with sqlite_backed():
        assert db.db_probe("target")["reachable"] is True

    with db_that_raises(OSError("connection refused")):
        out = db.db_probe("target")
        assert out["reachable"] is False
        assert "connection refused" in out["note"]
        assert "has NOT cleared" in out["note"]

    assert db.db_probe("target")["reachable"] is None     # unconfigured


def test_a_count_is_derived_from_the_statements_own_predicate():
    """A stub returning a fixed number could not catch this regression.

    The count must describe the SAME rows the DELETE will touch. Here the
    table holds eight rows and only three match, so a count query built
    from a different predicate returns a visibly wrong number.
    """
    rows = [(i, "b-99" if i < 3 else "b-01", "x") for i in range(8)]
    with sqlite_backed(rows=rows):
        found = target_repair.inspect(
            "DELETE FROM orders WHERE batch_id = 'b-99'")
        assert found["count_query"] == (
            "SELECT COUNT(*) FROM orders WHERE batch_id = 'b-99'")
        assert target_repair.count_rows(found["count_query"]) == 3


def test_a_repair_executes_and_reports_what_it_actually_changed():
    """Acceptance 8, and the sqlalchemy import that made it impossible."""
    rows = [(i, "b-99" if i < 4 else "b-01", "x") for i in range(10)]
    with sqlite_backed(rows=rows) as database:
        plan = target_repair.plan_repair(
            "DELETE FROM orders WHERE batch_id = 'b-99'",
            verdict(fault_zone="target", confidence="high",
                    likely_cause="a re-read duplicated four rows"),
            incident="i-1")

        assert plan["will_execute"] is True, plan["reason"]
        assert plan["rows_affected"] == 4, plan

        result = target_repair.execute(plan)
        assert result["rows_affected"] == 4, result
        assert result["matched_prediction"] is True

        left = database.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert left == 6, left


def test_a_failed_statement_rolls_back_and_says_so():
    rows = [(1, "b-99", "x")]
    with sqlite_backed(rows=rows) as database:
        try:
            db.target().execute("DELETE FROM no_such_table WHERE id = 1")
        except Exception as error:
            assert "no_such_table" in str(error)
        else:
            raise AssertionError("a bad statement did not raise")

        assert database.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1


def test_an_uncountable_repair_says_so_rather_than_guessing_zero():
    with db_that_raises(OSError("server closed the connection")):
        plan = target_repair.plan_repair(
            "DELETE FROM orders WHERE batch_id = 'b-99'",
            verdict(fault_zone="target", confidence="high",
                    likely_cause="duplicates from a re-read"),
            incident="i-2")
        assert plan["rows_affected"] is None
        assert "could not count" in plan["reason"], plan["reason"]


def test_the_source_connection_has_no_way_to_write():
    """Acceptance 9. Three independent layers, tested individually."""
    # 1. There is no method to call.
    assert not hasattr(db.SourceDB("postgresql://x/y"), "execute")
    assert hasattr(db.TargetDB("postgresql://x/y"), "execute")

    # 2. The statement allowlist refuses anything that is not a read.
    with sqlite_backed(source_url="postgresql://test/src"):
        source = db.source()
        for statement in ("DELETE FROM orders WHERE id = 1",
                          "UPDATE orders SET name = 'x' WHERE id = 1",
                          "INSERT INTO orders VALUES (1,'a','b')",
                          "DROP TABLE orders",
                          "TRUNCATE orders"):
            try:
                source.query(statement)
            except db.WriteRefused:
                pass
            else:
                raise AssertionError(f"source allowed: {statement}")

        assert source.query("SELECT 1 AS n") == [{"n": 1}]

    # 3. The connection itself is opened read-only.
    opened = {}
    original = db.CONNECT
    db.CONNECT = lambda url, readonly=False: opened.setdefault("readonly", readonly)
    try:
        try:
            db.SourceDB("postgresql://x/y").query("SELECT 1")
        except Exception:
            pass
        assert opened["readonly"] is True
    finally:
        db.CONNECT = original


def test_a_table_name_that_is_not_a_table_name_never_reaches_sql():
    for hostile in ("orders; DROP TABLE users",
                    "orders WHERE 1=1 --",
                    "orders/*x*/",
                    "", None, "a b"):
        try:
            db.qualify(hostile)
        except ValueError:
            pass
        else:
            raise AssertionError(f"qualify accepted {hostile!r}")

    assert db.qualify("orders") == '"orders"'
    assert db.qualify("public.orders") == '"public"."orders"'
    assert db.qualify('"public"."orders"') == '"public"."orders"'


def test_reconcile_names_the_direction_of_the_drift():
    with sqlite_backed(rows=[(1, "a", "x")], table="orders",
                       source_url="postgresql://test/src"):
        out = db.reconcile("orders", "orders")
        assert out["drift"] == 0
        assert "same number of rows" in out["note"]

    # An unreadable side must never be reported as agreement.
    with db_that_raises(OSError("down")):
        out = db.reconcile("orders", "orders")
        assert out["drift"] is None
        assert "not 'the counts match'" in out["note"]


def test_the_agent_refuses_to_restart_into_a_missing_table():
    """Acceptance 2: probe says missing, refusal names the table."""
    ev = {"target_db": {"configured": True, "reachable": True},
          "target_table": {"table": "public.target", "exists": False}}
    reason = nodes.cause_still_present(ev)
    assert reason and "public.target" in reason, reason
    assert "nothing to write to" in reason


def test_a_dropped_replication_slot_says_fresh_initial_load():
    """Acceptance 4."""
    ev = {"replication_slot": {"configured": True, "exists": False,
                               "slots": []}}
    reason = nodes.cause_still_present(ev)
    assert reason and "fresh initial load" in reason, reason
    assert "no position to resume from" in reason


def _read_slot(pending):
    original = db.CONNECT
    db.CONNECT = lambda url, readonly=False: _slot_rows([
        {"slot_name": "striim_slot", "plugin": "pgoutput",
         "slot_type": "logical", "active": False,
         "restart_lsn": "0/3A2B1C0", "confirmed_flush_lsn": "0/3A2B1C0",
         "server_lsn": "0/3A2B1C0", "pending_bytes": pending}])
    config.SOURCE_DB_URL = "postgresql://test/src"
    try:
        return db.db_replication_slot_status()
    finally:
        db.CONNECT, config.SOURCE_DB_URL = original, None


def test_an_inactive_slot_with_work_waiting_is_reported():
    """Acceptance 5: active=false while the app claims to be running."""
    out = _read_slot(4_194_304)

    assert out["exists"] is True
    assert out["inactive"] == ["striim_slot"]

    # Both LSNs come back, because the caller needs their MOVEMENT. The
    # gap on its own is not a backlog and the note must not claim it is.
    assert out["server_lsn"] and out["confirmed_flush_lsn"]
    assert out["pending_bytes"] == 4_194_304
    assert "THIS IS NOT BY ITSELF A BACKLOG" in out["note"]
    assert "gap GROWING across checks" in out["note"]
    assert "running and reading nothing" not in out["note"]


def test_an_inactive_slot_with_nothing_waiting_is_normal():
    """The correction. A slot with nothing to stream is idle, not broken.

    This is what raised a false incident: every symptom of a stall -
    inactive slot, frozen LSN, no throughput - shown by a healthy pipeline
    whose source nobody was writing to.
    """
    out = _read_slot(0)

    assert out["inactive"] == ["striim_slot"]      # still true...
    assert out["pending_bytes"] == 0               # ...and still fine
    assert "Nothing is waiting to be read" in out["note"]
    assert "normal when there is nothing to stream" in out["note"]
    assert "running and reading nothing" not in out["note"]


def _slot_rows(rows):
    """A connection that answers any query with these rows."""
    columns = list(rows[0])

    class Cursor:
        description = [(c,) for c in columns]

        def execute(self, sql, params=()):
            return self

        def fetchall(self):
            return [tuple(r[c] for c in columns) for r in rows]

    class Handle:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    return Handle()


def test_the_cause_check_never_turns_i_could_not_look_into_a_finding():
    """Unconfigured and unreachable-to-check are not evidence."""
    assert nodes.cause_still_present({}) is None
    assert nodes.cause_still_present(
        {"target_db": {"configured": False, "reachable": None}}) is None
    assert nodes.cause_still_present(
        {"target_table": {"table": "x", "exists": None}}) is None
    assert nodes.cause_still_present(
        {"replication_slot": {"configured": False}}) is None


def test_sqlalchemy_is_not_imported_anywhere():
    """It is not installed, and importing it silently disabled every repair."""
    import pathlib
    package = pathlib.Path(__file__).resolve().parents[1] / "agent"
    for path in package.glob("*.py"):
        assert "sqlalchemy" not in path.read_text(encoding="utf-8"), path


# ================================================ EXCEPTION STORE ROWS

def test_the_agent_reads_the_rejected_rows_itself():
    """Acceptance 1. The row and the constraint, not 'go and look'.

    Live, Striim's UI showed `duplicate key value violates unique
    constraint "target_pkey", Key (id)=(9) already exists` and the agent
    said "Inspect exception store rows to find rejection cause". It told a
    human to read something it could read itself.
    """
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    out = st.get_rejected_rows(striim, REJECTING)

    assert out["shape"] == "rows", out
    assert out["row_count"] == 4, out
    assert out["statement"].startswith("SELECT * FROM "), out
    assert "duplicate key" in out["note"], out["note"]
    assert "never tell a human" in out["note"], out["note"]

    blob = json.dumps(out["rows"])
    assert "target_pkey" in blob and "(id)=(9)" in blob, blob


def test_the_rejection_note_reports_facts_not_a_dead_end():
    """The probe settled it: the rows are NOT readable over REST.

    So the note must not send the agent off to fetch them - that costs a
    round trip, creates a transient application in the Global namespace,
    and comes back with the statement the note could have carried all
    along. It reports the rejection, the age, the rate, and the statement.
    """
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    out = st.get_rejected_events(striim, REJECTING)
    note = out["note"]

    assert "a human can run" not in note, note
    assert "Call get_rejected_rows" not in note, note

    assert "cannot be read through this API" in note, note
    assert "query handle rather than data" in note, note
    assert f"SELECT * FROM {REJECTING}_ExceptionStore LIMIT 5;" in note, note
    assert out["read_rows_with"].startswith("SELECT * FROM"), out
    assert "238s ago" in note, note          # the age is in the sentence

    # ...and the model is still told never to delegate a read it can do.
    assert "NEVER ASK A HUMAN TO LOOK SOMETHING UP" in prompts.SYSTEM


def test_the_exception_store_is_not_queried_on_every_incident():
    """One transient Global application per incident, for nothing."""
    ev = evidence_for("rejected_rows.json", REJECTING)
    assert ev["rejected_events"]["has_ever_rejected"] is True
    assert "rejected_rows" not in ev, "the store was queried automatically"


def test_no_exception_store_is_not_an_empty_one():
    """An app with no store at all must not report zero rejected rows."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.get_rejected_rows(striim, "sarah_moolai.NeverSeen")
    assert out["shape"] == "no_store", out
    assert out["rows"] == []
    assert "not captured anywhere" in out["note"]


def test_every_unreadable_shape_hands_back_the_literal_statement():
    """Never 'check X'. Always the statement, and why it could not run."""
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    seed = {"app": REJECTING, "store_name": "x_ExceptionStore",
            "statement": "SELECT * FROM x_ExceptionStore LIMIT 5;",
            "rows": [], "row_count": 0}

    cases = {
        "error": [{"executionStatus": "Failure", "responseCode": 500,
                   "output": "not supported"}],
        "handle": [{"executionStatus": "Success", "responseCode": 200,
                    "output": ("adhocquery1f19293c4555311b6f622ca1b2e804a "
                               "QUERY 1 01f19293-c455-ef51-b6f6-22ca1b2e804a")}],
        # Anything handle-ISH but not the real format is 'unknown', not a
        # guess. The parser matches the observed shape exactly.
        "unknown_handleish": [{"executionStatus": "Success",
                               "responseCode": 200,
                               "output": "queryId=adhocquery1f19"}],
        "unknown": [{"executionStatus": "Success", "responseCode": 200,
                     "output": {"unexpected": "shape"}}],
    }
    for expected, raw in cases.items():
        out = st._classify_rows(dict(seed), raw)
        assert out["shape"] == expected.split("_")[0], (expected, out)
        assert seed["statement"] in out["note"], out["note"]

    # A formatted table is readable even though it is not structured.
    table = [{"executionStatus": "Success", "responseCode": 200,
              "output": "+----+--------+\n| id | reason |\n+----+--------+"}]
    out = st._classify_rows(dict(seed), table)
    assert out["shape"] == "text"
    assert "id | reason" in out["raw"]


def test_the_query_handle_is_parsed_not_treated_as_opaque():
    """The exact string a live SELECT returned.

        adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 01f19293-...

    Four fields: the ad-hoc query application Striim compiled the statement
    into, the word QUERY, an index, and a UUID. Parsing it means the
    incident can name the query, and means wiring up a results endpoint
    later is one function rather than a fresh investigation.
    """
    live = ("adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 "
            "01f19293-c455-ef51-b6f6-22ca1b2e804a")

    handle = st.parse_query_handle(live)
    assert handle["query_name"] == "adhocquery1f19293c4555311b6f622ca1b2e804a"
    assert handle["uuid"] == "01f19293-c455-ef51-b6f6-22ca1b2e804a"
    assert handle["index"] == 1
    # Global, not our namespace - which is why SELECT is not a read.
    assert handle["fq_name"] == ("Global.adhocquery"
                                 "1f19293c4555311b6f622ca1b2e804a")

    for not_a_handle in ("", "some rows", "{\"a\": 1}",
                         "adhocquery123 QUERY", "QUERY 1 abc"):
        assert st.parse_query_handle(not_a_handle) is None, not_a_handle


def test_a_handle_response_still_hands_over_the_statement():
    """The honest fallback, until a results endpoint is found.

    Better than the old behaviour by exactly the margin that matters: it
    says what it ran, what came back, and where to run it - rather than
    telling a human to go and inspect something.
    """
    seed = {"app": REJECTING, "store_name": f"{REJECTING}_ExceptionStore",
            "statement": f"SELECT * FROM {REJECTING}_ExceptionStore LIMIT 5;",
            "rows": [], "row_count": 0}
    raw = [{"executionStatus": "Success", "responseCode": 200,
            "output": ("adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 "
                       "01f19293-c455-ef51-b6f6-22ca1b2e804a")}]

    out = st._classify_rows(dict(seed), raw)

    assert out["shape"] == "handle"
    assert out["query"]["uuid"] == "01f19293-c455-ef51-b6f6-22ca1b2e804a"
    assert seed["statement"] in out["note"]
    assert "ad-hoc query application" in out["note"]
    assert "has not found yet" in out["note"]

    # rows: [] here means "not read", and the shape says which.
    assert out["rows"] == [] and out["shape"] != "rows"

    # And it never pretends a human should go rummaging.
    assert not nodes._is_a_lookup(out["note"]) or "Run this" in out["note"]


def test_an_empty_store_is_not_an_unreadable_one():
    """rows: [] must never mean 'I could not read them'."""
    seed = {"app": REJECTING, "store_name": "x", "statement": "SELECT 1;",
            "rows": [], "row_count": 0}
    out = st._classify_rows(dict(seed),
                            [{"executionStatus": "Success",
                              "responseCode": 200, "output": []}])
    assert out["shape"] == "rows"
    assert "not 'I could not read it'" in out["note"]


def test_the_row_limit_is_capped_in_code_not_trusted_to_the_caller():
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    for asked, expected in ((5, 5), (0, 5), (9999, st.MAX_REJECTED_ROWS),
                            (-3, 1)):
        out = st.get_rejected_rows(striim, REJECTING, limit=asked)
        assert f"LIMIT {expected};" in out["statement"], (asked, out["statement"])


def test_select_is_not_a_verb_the_model_can_reach():
    """A console SELECT creates a transient Global.adhocquery application.

    That is a write, in the namespace this agent is otherwise locked out
    of. So there is no general SELECT tool, and console() refuses it.
    """
    assert "SELECT" not in st.READ_ONLY_VERBS

    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    try:
        st.console(striim, "SELECT * FROM anything;")
    except st.NotReadOnly as error:
        assert "not a read-only verb" in str(error)
    else:
        raise AssertionError("console() allowed an arbitrary SELECT")

    # The one SELECT the agent issues is built in code from a verified
    # store name - the model supplies an app name and nothing else.
    schema = next(s for s in prompts.TOOL_SCHEMAS
                  if s["name"] == "get_rejected_rows")
    assert set(schema["input_schema"]["properties"]) == {"app", "limit"}


# ============================================ PROPERTY CHANGE CYCLE

def test_a_property_change_refuses_when_a_redeploy_could_re_read():
    """UNDEPLOY may discard the recovery position. For a file reader that
    makes the closing START a cold start - and a cold start re-reads.

    Refusing here is not caution. It is restart_risk applied one level up,
    and the five statements are handed over so a person can run them with
    their eyes open.
    """
    evidence = evidence_for("trial5_halt.json")
    out = act.alter_component_property(
        st.Striim.offline(FIXTURES / "trial5_halt.json"),
        TRIAL5, f"{TRIAL5}_FileReaderSource",
        "positionbyeof", "true", evidence=evidence)

    assert out["ok"] is False
    assert "171,000" in out["refused"], out["refused"]
    assert "duplicates it into the target" in out["refused"]
    assert out["steps"] == []

    # ...and the statements are still produced, in order, ready to paste.
    assert [s.split()[0] for s in out["statements"]] == list(act.ALTER_CYCLE)
    assert out["statements"][2].startswith(
        f"ALTER {TRIAL5}_FileReaderSource SET positionbyeof =")


def test_a_property_change_aborts_before_start_if_the_position_vanished():
    """The guard that works whether or not I understand UNDEPLOY."""
    assert act._position_lost(171000, None) is True
    assert act._position_lost(171000, 0) is True
    assert act._position_lost(171000, 4000) is True
    assert act._position_lost(171000, 171000) is False
    assert act._position_lost(171000, 180000) is False
    assert act._position_lost(None, None) is False      # nothing to lose


def test_a_property_change_never_leaves_an_app_undeployed():
    """If ALTER or DEPLOY fails, it deploys back before reporting."""
    sent = []

    class Recording:
        is_offline = False
        token = "t"

        def invalidate(self):
            pass

    def fake_send(striim, statement, timeout=30):
        sent.append(statement)
        if statement.startswith("ALTER"):
            raise RuntimeError("TQL parse error")
        return [{"executionStatus": "Success"}]

    original_send, original_dry = st._send, config.DRY_RUN
    original_pos = act._recorded_position
    st._send = fake_send
    config.DRY_RUN = False
    act._recorded_position = lambda s, c: 0
    try:
        out = act.alter_component_property(
            Recording(), TRIAL5, f"{TRIAL5}_Source", "batchpolicy", "x",
            evidence={"flow": {"source": {"profile":
                                          {"behaviour": "cdc_reader"}}}},
            verbose=False)
    finally:
        st._send, config.DRY_RUN = original_send, original_dry
        act._recorded_position = original_pos

    assert out["ok"] is False
    assert out["failed_at"] == "ALTER"
    assert out["restored"] is True, out
    assert sent[-1] == f"DEPLOY {TRIAL5};", sent
    assert "redeployed to avoid leaving it undeployed" in out["note"]


def test_an_app_the_agent_left_undeployed_is_not_silence():
    """DEPLOYED is a calm state. Without this the agent breaks a pipeline
    and then never mentions it again."""
    services = services_for("real_trial3_stopped.json")
    services.left_undeployed[TRIAL3] = "ALTER failed part-way"

    striim = st.Striim.offline(FIXTURES / "real_trial3_stopped.json")
    original = st.get_status
    st.get_status = lambda s, a: {"app": a, "status": "DEPLOYED",
                                  "is_fault": False, "status_means": "x"}
    try:
        out = nodes.triage({"app": TRIAL3, "status": "DEPLOYED",
                            "evidence": {}}, services)
    finally:
        st.get_status = original

    assert out["silent"] is False, out
    assert out["evidence"]["left_undeployed"] == "ALTER failed part-way"


def test_alter_will_not_interpolate_anything_that_is_not_an_identifier():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    hostile = [("batch policy", "x"), ("prop; DROP", "x"),
               ("prop", "x'; STOP app; --"), ("prop", "a\nb")]

    for prop, value in hostile:
        try:
            act.alter_component_property(striim, TRIAL5, f"{TRIAL5}_S",
                                         prop, value, evidence={})
        except act.ActionRefused:
            pass
        else:
            raise AssertionError(f"accepted {prop!r} = {value!r}")


def test_alter_is_not_in_the_lifecycle_verb_allowlist():
    """A narrow function builds the statement; the generic path must not."""
    assert "ALTER" not in act.ALLOWED_VERBS

    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    try:
        act.run_command(striim, TRIAL5, "ALTER")
    except act.ActionRefused as error:
        assert "not one of the allowed verbs" in str(error)
    else:
        raise AssertionError("run_command accepted ALTER")


# ========================================= CONSOLE READS AND PREFLIGHT

def test_scenario_7_console_read_refuses_anything_that_is_not_a_read():
    """Acceptance 7. The verb allowlist, and SELECT held out separately."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    for statement in ("DROP TABLE x", "STOP sarah_moolai.Trial5",
                      "UNDEPLOY sarah_moolai.Trial5", "ALTER APPLICATION x",
                      "CREATE APPLICATION y", "RESUME x"):
        out = st.console_read(striim, statement)
        assert out["shape"] == "refused", (statement, out)
        assert "not a read" in out["note"], out["note"]

    # SELECT is refused for a different reason, and says which.
    out = st.console_read(striim, "SELECT * FROM anything")
    assert out["shape"] == "refused"
    assert "Global namespace" in out["note"], out["note"]
    assert "get_rejected_rows" in out["note"]


def test_console_read_passes_structured_output_through_unchanged():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    out = st.console_read(striim,
                          "DESCRIBE sarah_moolai.Trial5_FileReaderSource;")
    assert out["shape"] == "structured", out
    assert out["output"][0]["adapterName"] == "FileReader"


def test_a_formatted_table_is_reported_as_text_not_mis_parsed():
    """No box-drawing parser exists until a real response needs one."""
    parsed = st._parse_console(
        [{"executionStatus": "Success",
          "output": "+--------+-------+\n| comp   | rate  |\n+--------+-------+"}],
        "MON x;")
    assert parsed["shape"] == "text"
    assert "read it directly" in parsed["note"]
    assert "| comp" in parsed["raw"]


def test_preflight_catches_a_pipeline_that_feeds_itself():
    """Acceptance 10. The target writes back into its own source table."""
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    out = st.preflight(striim, REJECTING)

    checks = {f["check"] for f in out["findings"]}
    assert "self_replication" in checks, out["findings"]
    assert out["clean"] is False

    finding = next(f for f in out["findings"]
                   if f["check"] == "self_replication")
    assert "public.source" in finding["note"], finding
    assert "feeds itself" in finding["note"]
    assert "None of them will show up as a fault" in out["note"]


def test_preflight_flags_a_wildcard_target_mapping():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    original = st.describe_component
    st.describe_component = lambda s, c: {
        "component": c,
        "properties": ({"tables": '"public"."%",public.%'}
                       if "Target" in c else {"tables": "public.source"})}
    try:
        out = st.preflight(striim, TRIAL5)
    finally:
        st.describe_component = original

    checks = {f["check"] for f in out["findings"]}
    assert "table_mapping" in checks, out["findings"]
    finding = next(f for f in out["findings"] if f["check"] == "table_mapping")
    assert "writes its output back into its own input" in finding["note"]


def test_a_target_is_never_told_it_might_restart_from_the_beginning():
    """Observed: read_position null on a DatabaseWriter produced

        "no read position recorded ... which for a source means a restart
         begins from the beginning"

    A writer has no read position and never re-reads. That sentence
    describes a risk that does not exist on a target, and sends whoever
    reads it looking for one.
    """
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")

    target = st.describe_component(striim, f"{REJECTING}_Target")
    assert target["role"] == "target"
    assert target["read_position"] is None
    assert "restart begins from the beginning" not in (target["note"] or "")
    assert "configured to write to" in target["note"], target["note"]
    assert "check the target system" in target["note"]

    # ...and a source with no position still gets the warning, because
    # there it is true.
    source = st.describe_component(striim, f"{REJECTING}_PostgresCDC")
    assert source["role"] == "source"
    assert "configured to read" in source["note"], source["note"]


def test_preflight_warns_about_a_batch_policy_that_never_flushes():
    """EventCount:1000,Interval:60 on a trickle - events in, nothing out."""
    striim = st.Striim.offline(FIXTURES / "rejected_rows.json")
    out = st.preflight(striim, REJECTING)

    finding = next((f for f in out["findings"]
                    if f["check"] == "batch_policy"), None)
    assert finding, out["findings"]
    assert "1,000 events" in finding["note"], finding["note"]
    assert "low-volume" in finding["note"]

    # The source is idle in this fixture, so it is called out as certain
    # rather than possible.
    assert finding["severity"] == "high", finding
    assert "will never reach 1,000" in finding["note"], finding["note"]


def test_a_batch_policy_with_no_interval_is_the_dangerous_one():
    """A timer eventually flushes. A pure event count never does."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    original = st.describe_component
    st.describe_component = lambda s, c: {
        "component": c,
        "properties": {"batchpolicy": "EventCount:5000"} if "Target" in c
                      else {"tables": "public.source"}}
    try:
        findings = st._batch_policy_findings(
            striim, "ns.App_Target",
            {"source": {"event_rate": 2.0}})
    finally:
        st.describe_component = original

    assert findings and findings[0]["severity"] == "high"
    assert "no time-based interval" in findings[0]["note"]
    assert "buffer forever" in findings[0]["note"]
    assert "Add an interval" in findings[0]["note"]


def test_a_small_batch_policy_is_not_flagged():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    original = st.describe_component
    st.describe_component = lambda s, c: {
        "component": c, "properties": {"batchpolicy": "EventCount:10,Interval:5"}}
    try:
        assert st._batch_policy_findings(
            striim, "ns.App_Target", {"source": {"event_rate": 2.0}}) == []
    finally:
        st.describe_component = original


def test_preflight_reports_and_never_blocks():
    """It returns findings. There is no path by which it stops anything."""
    striim = st.Striim.offline(FIXTURES / "trial5_running.json")
    out = st.preflight(striim, TRIAL5)
    assert isinstance(out["findings"], list)
    assert set(out) >= {"app", "checks_run", "findings", "clean", "note"}


# ==================================================== REGISTRY DRIFT

def test_every_tool_has_a_schema_and_every_schema_has_a_tool():
    """The bug that hid describe_pipeline from the model for three tasks.

    It was in TOOLS with no entry in TOOL_SCHEMAS, so the model could never
    call it. Two instances of this pattern is a class, not an accident.
    """
    tools = set(st.TOOLS) | set(db.TOOLS)
    schemas = {s["name"] for s in prompts.TOOL_SCHEMAS}
    assert tools == schemas, {"tools only": tools - schemas,
                              "schemas only": schemas - tools}

    # The two registries must not overlap - they have different calling
    # conventions and the dispatcher tries Striim first.
    assert not (set(st.TOOLS) & set(db.TOOLS))


def test_every_verdict_field_the_code_reads_is_asked_for_in_the_prompt():
    """`target_sql` was read by _decide_target and never requested.

    So the target-repair branch could not fire, whatever else was fixed.
    """
    import inspect as _inspect
    import re as _re

    source = (_inspect.getsource(nodes.decide)
              + _inspect.getsource(nodes._decide_target)
              + _inspect.getsource(nodes.escalate))
    read = set(_re.findall(r"verdict\.get\(\s*[\"'](\w+)[\"']", source))

    # Fields the code invents rather than asks for.
    computed = {"fault_zone", "_composed_by_code"}
    for field in read - computed:
        assert field in prompts.SYSTEM, (
            f"decide() reads verdict['{field}'] but the prompt never asks "
            f"for it — the branch that uses it can never fire")


# ============================================== TOOLS AND GUARDRAILS

def test_tools_distinguish_no_data_from_no():
    """An empty result is not evidence of absence."""
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")

    history = st.get_state_history(striim, "sarah_moolai.NeverSeen")
    assert history["looks_deliberate"] is None
    assert "NO DATA" in history["note"]

    rejected = st.get_rejected_events(striim, "sarah_moolai.NeverSeen")
    assert rejected["has_ever_rejected"] is None
    assert "not captured anywhere" in rejected["note"]

    status = st.get_status(striim, "sarah_moolai.NeverSeen")
    assert status["status"] == "NOT FOUND"
    assert status["is_fault"] is False


def test_never_is_different_from_not_reported():
    """A literal 0 timestamp means 'never', which is a finding in itself."""
    assert st._has_ever(0) is False
    assert st._has_ever(None) is None
    assert st._has_ever(1785787475154) is True

    striim = st.Striim.offline(FIXTURES / "real_oracle_cdc_terminated.json")
    flow = st.get_flow_stats(striim, ORACLE)
    assert flow["source"]["has_ever_read"] is False, flow["source"]


def test_a_recorded_checkpoint_is_not_a_safe_checkpoint():
    """Trial5: recovery on, checkpoints written, and still not safe."""
    ev = evidence_for("trial5_halt.json")
    assert ev["checkpoints"]["has_usable_checkpoint"] is True
    risk = nodes.restart_risk(ev)
    assert risk is not None
    assert "171,000" in risk and "duplicates every row" in risk, risk

    # Short enough to be the second line of a message read on a phone.
    assert len(risk.split()) <= 15, risk


def test_a_downstream_fault_does_not_block_a_restart():
    """The rule must not refuse everything, or it is not a rule."""
    ev = evidence_for("staged_safe_restart.json")
    assert nodes.restart_risk(ev) is None


def test_the_definition_edit_time_cannot_be_turned_into_a_timeline():
    """Observed live, and it is the exact failure this design prevents:

        "Terminated shortly after config edit. App terminated; definition
         was edited about 16 minutes before fault."

    There was no relationship — the app had just been redeployed. The model
    assembled a plausible cause out of a coincidence, and it could do that
    because the tool handed it a number to subtract from.

    So the number is gone. The field is an absolute wall-clock string,
    renamed, and carries a warning the model reads next to the value. You
    cannot unit-test what a model concludes, but you CAN remove the
    arithmetic that made the conclusion available.
    """
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    status = st.get_status(striim, TRIAL5)

    # No relative age anywhere in the output, under any spelling.
    for key, value in status.items():
        if "definition" in key and "warning" not in key:
            assert not isinstance(value, (int, float)), (key, value)
        assert "last_edited" not in key, key

    assert isinstance(status["definition_edited_at"], str)
    warning = status["definition_edited_at_warning"]
    assert "not related to this fault" in warning.lower(), warning
    assert "never cite it as a cause" in warning.lower(), warning
    assert "get_state_history" in warning

    # And the model is told the same thing in the system prompt.
    assert "NEVER BUILD A TIMELINE" in prompts.SYSTEM
    assert "definition_edited_at" in prompts.SYSTEM

    # Meanwhile the real fault age is unknown, because no transition was
    # recorded — which is the honest answer and the one that stays.
    assert st.fault_age(striim, TRIAL5) is None


def test_the_evidence_wait_is_skipped_on_a_cluster_with_no_error_text():
    """Three minutes of dead air, every time, waiting for nothing.

    The wait exists because Striim publishes error text after the status
    change. It does not exist to wait out a cluster that publishes none at
    all — and this one publishes none for any of its 576 applications.
    """
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    errors = st.get_errors(striim, TRIAL5)
    assert errors["cluster_publishes_errors"] is False
    assert nodes._text_might_still_arrive(errors) is False

    # Where text really can still arrive, it still waits.
    assert nodes._text_might_still_arrive(
        {"fault_age_seconds": 30}) is True
    assert nodes._text_might_still_arrive(
        {"fault_age_seconds": 90_000}) is False

    # And the default is two short waits, not four long ones.
    assert config.EVIDENCE_ATTEMPTS * config.EVIDENCE_WAIT_SECONDS <= 60


def test_the_read_only_layer_cannot_write():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    for statement in ("STOP sarah_moolai.Trial5;", "DROP APPLICATION x;",
                      "UNDEPLOY sarah_moolai.Trial5;"):
        try:
            st.console(striim, statement)
        except st.NotReadOnly:
            pass
        else:
            raise AssertionError(f"read-only layer allowed: {statement}")


def test_only_the_allowed_verbs_run():
    striim = st.Striim.offline(FIXTURES / "trial5_halt.json")
    try:
        act.run_command(striim, TRIAL5, "DROP")
    except act.ActionRefused as error:
        assert "not one of the allowed verbs" in str(error)
    else:
        raise AssertionError("the action layer allowed DROP")


# ================================================ CONNECTOR PROFILES

def test_profiles_resolve_real_cluster_component_names():
    """Every one of these is a real component name from the capture."""
    cases = [
        ("sarah_moolai.Trial5_FileReaderSource", "source", "file_reader"),
        ("sarah_moolai.File_Trial5_Target", "target", "append_only_writer"),
        ("MANIMARAN22.OracleToFabricDWH_CDC_OracleSource", "source", "cdc_reader"),
        ("MANIMARAN22.OracleToFabricDWH_CDC_Target", "target", "append_only_writer"),
        ("harrybwatson.Vercel_to_Azure_SQL_CDC_PostgresCDC", "source", "cdc_reader"),
        ("guhan.MyNeoPipelineKafkaToS3_Source", "source", "stream_reader"),
        ("cristinamclarkin.SalesforceCDC_Source", "source", "api_poller"),
        ("jobinthompu.BQ2Icerberg_BigQuerySource", "source", "batch_reader"),
        ("clifford.sheets2bq2_Source", "source", "spreadsheet_reader"),
    ]
    for name, role, expected in cases:
        assert profiles.behaviour_of(name, role) == expected, (name, role)


def test_the_role_confines_the_search_it_does_not_merely_order_it():
    """A target with 'Oracle' in its name is not an Oracle CDC reader."""
    assert profiles.behaviour_of(
        "MANIMARAN22.OracleToFabricDWH_CDC_Target", "target") != "cdc_reader"
    assert profiles.behaviour_of(
        "ns.OracleMigration_FileReaderSource", "source") == "file_reader"


def test_an_unknown_connector_says_so_instead_of_guessing():
    profile = profiles.profile_for("ns.App_QuantumFluxSource", role="source")
    assert profile["behaviour"] == "unknown"
    assert profile["silence_is_normal"] is None
    assert "I do not know" in profile["restart_risk"]


def test_a_real_adapter_name_beats_the_component_name():
    """DESCRIBE says FileWriter; the component name says nothing useful."""
    by_name = profiles.profile_for("sarah_moolai.File_Trial5_Target",
                                   role="target")
    by_adapter = profiles.profile_for("sarah_moolai.File_Trial5_Target",
                                      role="target", adapter="FileWriter")
    assert by_adapter["matched_on"] == "filewriter", by_adapter
    assert by_name["behaviour"] == by_adapter["behaviour"] == "append_only_writer"


def test_restart_risk_states_the_condition_not_the_conclusion():
    """A flat 'HIGH RISK' drowns out the evidence that would resolve it."""
    risk = profiles.BEHAVIOUR_PROFILES["file_reader"]["restart_risk"]
    assert "get_checkpoints" in risk and "describe_component" in risk
    assert "DEPENDS ON THE CHECKPOINT" in risk


# ================================================== TARGET REPAIR (ZONE 2)

def test_unbounded_statements_are_never_executable():
    for sql in ("DELETE FROM orders",
                "TRUNCATE TABLE orders",
                "DROP TABLE orders",
                "UPDATE orders SET flag = 1",
                "DELETE FROM orders WHERE id = 1; DROP TABLE orders"):
        finding = target_repair.inspect(sql)
        assert not finding["executable"], (sql, finding)
        assert finding["refusals"], sql


def test_a_bounded_delete_derives_its_own_count_query():
    finding = target_repair.inspect(
        "DELETE FROM sales.orders WHERE batch_id = 'b-2026-08-05-11'")
    assert finding["executable"]
    assert finding["count_query"] == (
        "SELECT COUNT(*) FROM sales.orders "
        "WHERE batch_id = 'b-2026-08-05-11'")


def test_ddl_is_proposed_but_never_run():
    for sql in ("CREATE TABLE staging.orders (id INT)",
                "ALTER TABLE sales.orders ADD COLUMN region VARCHAR(20)"):
        finding = target_repair.inspect(sql)
        assert not finding["executable"], sql
        assert "never run by the agent" in finding["refusals"][0], finding


def test_a_repair_without_a_named_cause_is_refused():
    plan = target_repair.plan_repair(
        "DELETE FROM sales.orders WHERE batch_id = 'x'",
        verdict(fault_zone="target", likely_cause="unknown"),
        incident="i-1")
    assert plan["will_execute"] is False
    assert "guess" in plan["reason"], plan["reason"]


def test_a_repair_with_no_target_connection_is_only_proposed():
    """The shipped default. It writes the SQL out and stops."""
    assert config.TARGET_DB_URL is None
    plan = target_repair.plan_repair(
        "DELETE FROM sales.orders WHERE batch_id = 'b-99'",
        verdict(fault_zone="target", confidence="high",
                likely_cause="a half-written batch from the failed run"),
        incident="i-2")
    assert plan["will_execute"] is False
    assert "no target database is configured" in plan["reason"], plan["reason"]
    assert "DELETE FROM sales.orders" in target_repair.describe(plan)
    assert "not running it" in target_repair.describe(plan)


def test_target_faults_reach_zone_two_rather_than_a_restart():
    sql = "DELETE FROM sales.orders WHERE batch_id = 'b-99'"
    target_fault = verdict(fault_zone="target", confidence="high",
                           restart_would_help=True, why_not="",
                           likely_cause="a half-written batch",
                           target_sql=sql)
    final, notifier, _ = run("staged_safe_restart.json",
                             model=FakeModel(target_fault),
                             namespace="jobregonr")

    assert final["tier"] == 2, (final["tier"], final["reason"])
    assert final["repair_plan"]["sql"].startswith("DELETE FROM sales.orders")
    assert sql in notifier.everything          # rule 3: always shown


# ====================================================== REPAIR SKILLS
#
# TASK-3 section 5. The ones that matter are 3, 5, 7, 10 and 11 - every one
# of those is the code declining a repair the model asked for, which is the
# same argument as tier 4 one level down.

def state_from(fixture, app=None, **extra):
    """A decided-enough state for a skill to be asked about."""
    evidence = evidence_for(fixture, app)
    app = app or json.loads((FIXTURES / fixture).read_text())["captured_from"]
    striim = st.Striim.offline(FIXTURES / fixture)
    return {"app": app, "evidence": evidence,
            "status": st.get_status(striim, app)["status"], **extra}


def test_skill_1_resumes_when_the_dependency_is_back():
    """Scenario 1: HALT, dependency reachable, restart cannot re-read."""
    state = state_from("staged_safe_restart.json")
    state["evidence"]["dependency"] = {
        "component": "x", "kind": "tcp", "target": "sf.example.com:443",
        "reachable": True, "detail": "sf.example.com:443 accepted a connection"}

    ok, why = skills.SKILLS["wait_and_resume"].applies(state)
    assert ok, why

    plan = skills.SKILLS["wait_and_resume"].plan(state)
    assert plan.statements == [f"RESUME {SALESFORCE};"]
    assert plan.reversible is True


def test_skill_1_declines_when_the_dependency_is_still_down():
    """Scenario 2: the endpoint is named in the refusal, not just 'down'."""
    state = state_from("staged_safe_restart.json")
    state["evidence"]["dependency"] = {
        "component": "x", "kind": "tcp", "target": "sf.example.com:443",
        "reachable": False,
        "detail": "sf.example.com:443 refused the connection"}

    ok, why = skills.SKILLS["wait_and_resume"].applies(state)
    assert not ok
    assert "sf.example.com:443" in why, why


def test_skill_1_declines_on_an_untrustworthy_position():
    """Scenario 3: cites the position, not the checkpoint flag.

    Real Trial5: recovery on, checkpoints written, has_usable_checkpoint
    true — and the skill still declines, because the flag is not the thing
    that matters.
    """
    state = state_from("trial5_halt.json")
    assert state["evidence"]["checkpoints"]["has_usable_checkpoint"] is True

    ok, why = skills.SKILLS["wait_and_resume"].applies(state)
    assert not ok
    assert "171,000" in why and "duplicates every row" in why, why


def test_skill_3_needs_a_recurrence_and_never_reaches_tier_1():
    """Scenario 4: propose_ignorable_exception is tier 2, always."""
    skill = skills.SKILLS["propose_ignorable_exception"]
    assert skill.max_tier == 2, "this skill must never be able to reach tier 1"

    first = state_from("trial5_halt.json", fingerprint_seen=1)
    ok, why = skill.applies(first)
    assert not ok and "recurrent" in why, why


def test_skill_3_declines_when_the_failures_are_systemic():
    """Scenario 5: 40% of rows failing is not an isolated bad row."""
    skill = skills.SKILLS["propose_ignorable_exception"]
    state = state_from("trial5_halt.json", fingerprint_seen=3)
    state["evidence"]["rejected_events"] = {
        "exception_store_exists": True, "has_ever_rejected": True,
        "write_rate": 40.0, "store_name": f"{TRIAL5}_ExceptionStore"}
    state["evidence"]["flow"]["source"]["event_rate"] = 100.0

    ok, why = skill.applies(state)
    assert not ok
    assert "systemic" in why and "40%" in why, why


def test_skill_3_plans_with_the_row_count_and_says_rows_are_skipped():
    skill = skills.SKILLS["propose_ignorable_exception"]
    state = state_from("trial5_halt.json", fingerprint_seen=2)
    state["evidence"]["rejected_events"] = {
        "exception_store_exists": True, "has_ever_rejected": True,
        "write_rate": 0.5, "store_name": f"{TRIAL5}_ExceptionStore"}
    state["evidence"]["flow"]["source"]["event_rate"] = 100.0
    state["evidence"]["errors"] = {"errors": [
        "com.webaction.exception.AdapterException: bad row"], "error_count": 1}

    ok, why = skill.applies(state)
    assert ok, why

    plan = skill.plan(state)
    assert plan.counts["proportion_of_throughput"] == 0.005
    assert "AdapterException" in plan.summary
    assert "SKIPPED" in plan.effect
    assert any("business decision" in w for w in plan.warnings)


def test_skill_5_declines_on_an_append_only_target():
    """Scenario 7. This refusal is itself a finding worth showing.

    Real Trial5's target is a FileWriter. Nothing can deduplicate a file
    after the fact — which is exactly why the restart was refused rather
    than repaired.
    """
    state = state_from("trial5_halt.json", reread_confirmed=True)
    behaviour = state["evidence"]["flow"]["target"]["profile"]["behaviour"]
    assert behaviour == "append_only_writer"

    ok, why = skills.SKILLS["dedupe_target_rows"].applies(state)
    assert not ok
    assert "appends" in why and "permanent" in why, why


def test_skill_5_declines_without_a_key():
    """No primary key means no safe dedupe, whatever the model thinks."""
    state = state_from("trial5_halt.json", reread_confirmed=True)
    state["evidence"]["flow"]["target"]["profile"] = {
        "behaviour": "transactional_writer"}

    assert config.TARGET_PRIMARY_KEY is None
    ok, why = skills.SKILLS["dedupe_target_rows"].applies(state)
    assert not ok
    assert "primary key" in why, why


def test_skill_5_declines_without_a_confirmed_reread():
    state = state_from("trial5_halt.json")
    state["evidence"]["flow"]["target"]["profile"] = {
        "behaviour": "transactional_writer"}
    original = config.TARGET_PRIMARY_KEY
    config.TARGET_PRIMARY_KEY = "order_id"
    try:
        ok, why = skills.SKILLS["dedupe_target_rows"].applies(state)
    finally:
        config.TARGET_PRIMARY_KEY = original
    assert not ok
    assert "no re-read has been confirmed" in why, why


def test_skill_5_measures_before_it_proposes():
    """Scenario 6: a real count, or it is not executable."""
    state = state_from("trial5_halt.json", reread_confirmed=True)
    state["evidence"]["flow"]["target"]["profile"] = {
        "behaviour": "transactional_writer"}

    originals = (config.TARGET_PRIMARY_KEY, config.TARGET_TABLE)
    config.TARGET_PRIMARY_KEY, config.TARGET_TABLE = "order_id", "sales.orders"
    try:
        ok, why = skills.SKILLS["dedupe_target_rows"].applies(state)
        assert ok, why
        plan = skills.SKILLS["dedupe_target_rows"].plan(state)
    finally:
        config.TARGET_PRIMARY_KEY, config.TARGET_TABLE = originals

    assert plan.statements[0].startswith("DELETE FROM sales.orders WHERE")
    # No target database is configured, so the count could not be taken -
    # and an uncounted delete is never executable.
    assert plan.executable is False
    assert "count" in plan.reason.lower(), plan.reason
    assert plan.reversible is False


def test_skill_4_writes_ddl_out_and_never_runs_it():
    """Scenario 8."""
    skill = skills.SKILLS["recreate_missing_target_object"]
    state = state_from("trial5_halt.json")
    state["evidence"]["errors"] = {
        "error_count": 1,
        "errors": ["ORA-00942: table or view does not exist"]}

    ok, why = skill.applies(state)
    assert ok, why

    plan = skill.plan(state)

    # A whole missing TABLE cannot be reconstructed from a schema diff, so
    # this one is still written out for a person - unlike a missing column.
    assert plan.executable is False
    assert "could not be read from the source" in plan.reason, plan.reason

    # TASK-4 section 3.3: a finished statement, not a description of one.
    assert plan.command, plan
    assert plan.command.rstrip().endswith(";"), plan.command
    assert plan.run_on, plan
    assert plan.derived_from, plan


def test_a_missing_column_produces_a_pasteable_alter_statement():
    """The exact §3.3 example, built from what the agent actually read."""
    skill = skills.SKILLS["recreate_missing_target_object"]
    state = state_from("rejected_rows.json", REJECTING)
    state["evidence"]["errors"] = {
        "error_count": 1,
        "errors": ['ERROR: column "city" of relation "target" does not exist']}
    state["evidence"]["target_table"] = {"table": "public.target",
                                         "exists": True}
    state["evidence"]["source_table"] = {
        "columns": [{"column_name": "city", "data_type": "text"}]}

    ok, why = skill.applies(state)
    assert ok, why

    plan = skill.plan(state)
    assert plan.command == (
        'ALTER TABLE "public"."target" ADD COLUMN "city" text;'), plan.command
    assert plan.reversal == (
        'ALTER TABLE "public"."target" DROP COLUMN "city";'), plan.reversal
    assert "neondb" in plan.run_on, plan.run_on
    assert "measured schema difference" in plan.derived_from, plan.derived_from


def test_skill_7_escalates_with_the_exact_object_named():
    """Scenario 9: specificity is the entire value of this skill."""
    state = state_from("trial5_halt.json")
    state["evidence"]["dependency"] = {
        "component": "x", "kind": "file",
        "target": "UploadedFiles/sarah_moolai/AI.csv", "reachable": False,
        "detail": "UploadedFiles/sarah_moolai is visible and does not "
                  "contain AI.csv"}

    skill = skills.SKILLS["escalate_source_problem"]
    ok, why = skill.applies(state)
    assert ok, why

    plan = skill.plan(state)
    assert plan.executable is False
    assert "UploadedFiles/sarah_moolai/AI.csv" in plan.summary, plan.summary
    assert any("byte 171,000" in s for s in plan.statements), plan.statements
    assert "never writes to a source" in plan.effect or "does not write" in plan.effect


def test_scenario_10_a_skill_whose_preconditions_fail_is_dropped_and_explained():
    """The reason is posted, because it tells a human something real."""
    wrong = verdict(fault_zone="target", confidence="high",
                    restart_would_help=False,
                    suggested_skill="dedupe_target_rows")
    final, notifier, _ = run("trial5_halt.json", model=FakeModel(wrong))

    assert final["skill"] is None
    assert final["skill_plan"] is None
    assert "considered dedupe_target_rows" in final["skill_dropped"]
    assert "appends" in final["skill_dropped"]
    assert "dedupe_target_rows" in notifier.everything


def test_scenario_11_a_target_skill_on_a_source_fault_is_refused_by_zone():
    """The zone check runs BEFORE the preconditions are even read."""
    wrong = verdict(fault_zone="source", confidence="high",
                    suggested_skill="dedupe_target_rows")
    final, _, _ = run("trial5_halt.json", model=FakeModel(wrong))

    assert final["skill"] is None
    assert "repairs the target" in final["skill_dropped"], final["skill_dropped"]
    assert "fault is in the source" in final["skill_dropped"]
    assert final["tier"] == 3


def test_a_skill_that_does_not_exist_is_declined_not_crashed():
    wrong = verdict(fault_zone="pipeline", suggested_skill="reboot_the_universe")
    final, _, _ = run("trial5_halt.json", model=FakeModel(wrong))
    assert final["skill"] is None
    assert "does not exist" in final["skill_dropped"]
    assert final["tier"] == 3        # still refused for the real reason


def test_a_source_escalation_becomes_the_do_this_line():
    """Skill 7's output is the next action, verbatim, on the message."""
    suggested = verdict(fault_zone="source", confidence="high",
                        human_action_needed="",
                        suggested_skill="escalate_source_problem")
    final, notifier, _ = run("trial5_halt.json", model=FakeModel(suggested))

    assert final["skill"] == "escalate_source_problem"
    incident = notifier.posts[0]
    assert final["tier"] == 3

    # It names the exact object either way. What it does NOT do is claim
    # the file is missing: this cluster publishes no error text and the
    # path is not visible from here, so nothing has actually established
    # that. "Check it" is the honest instruction; "restore it" would send
    # somebody to look for a file that may be sitting right there.
    assert "UploadedFiles/sarah_moolai/AI.csv" in incident, incident
    assert "You need to: run `SELECT count(*) FROM " in incident, incident
    assert "could not determine" not in incident, incident

    # A physical action is NOT dressed up as a command to paste.
    assert "Run this on" not in incident, incident


def test_restore_is_only_claimed_when_something_is_known_to_be_missing():
    """"restore public.source" was invented for a stalled CDC pipeline
    whose source table was present and entirely healthy."""
    path = "UploadedFiles/sarah_moolai/AI.csv"

    # Nothing establishes the file is gone - so it names the one command
    # that would settle it, rather than apologising.
    unknown = skills._source_action(path, None, {})
    assert "SELECT count(*)" in unknown, unknown
    assert "could not determine" not in unknown

    # The error names it - now "restore" is earned.
    named = {"errors": {"error_count": 1, "errors": [
        "java.io.FileNotFoundException: UploadedFiles/sarah_moolai/AI.csv"]}}
    assert skills._source_action(path, None, named).startswith("restore")

    # A probe looked in the right place and did not find it.
    looked = {"kind": "file", "reachable": False, "target": path}
    assert skills._source_action(path, looked, {}).startswith("restore")

    # An inactive slot is not a missing table, and the action says so.
    stalled = {"replication_slot": {"inactive": ["striim_slot"]}}
    action = skills._source_action("public.source", None, stalled)
    assert "restart the pipeline so it reattaches to striim_slot" == action
    assert "restore" not in action


def test_every_skill_declares_preconditions_in_code():
    """TASK-3 section 6: no skill may exist that cannot state them."""
    for name, skill in skills.SKILLS.items():
        assert skill.zone in ("pipeline", "target", "source"), name
        assert skill.description, name
        assert skill.max_tier in (1, 2, 3), name
        assert skill.applies.__qualname__.split(".")[0] != "Skill", \
            f"{name} does not override applies()"


def test_the_model_is_offered_exactly_the_skills_the_code_will_run():
    """The catalogue is built from the registry, so it cannot drift."""
    prompt = prompts.system_prompt()
    for name in skills.SKILLS:
        assert name in prompt, name
    assert "{skills}" not in prompt


# ================================================= REACHABILITY PROBE

def test_the_probe_says_cannot_check_rather_than_guessing():
    """A server-side path is not visible from here. That is not 'missing'."""
    result = probes.check_dependency({
        "component": "ns.App_FileReaderSource",
        "expects": "UploadedFiles/sarah_moolai/AI.csv",
        "properties": {"directory": "UploadedFiles/sarah_moolai/"},
        "profile": {"behaviour": "file_reader"}})

    assert result["reachable"] is None, result
    assert "not visible from here" in result["detail"]
    assert "could not check" in probes.describe_reachability(result)


def test_the_probe_finds_a_file_that_is_really_there():
    present = _SCRATCH / "probe-present.csv"
    present.write_text("a,b,c\n")

    result = probes.check_dependency({
        "component": "ns.App_FileReaderSource",
        "expects": str(present),
        "properties": {"directory": str(_SCRATCH)},
        "profile": {"behaviour": "file_reader"}})

    assert result["reachable"] is True, result
    assert result["size_bytes"] > 0, result


def test_a_visible_directory_without_the_file_is_a_real_negative():
    """We looked in the right place. That IS evidence."""
    result = probes.check_dependency({
        "component": "ns.App_FileReaderSource",
        "expects": str(_SCRATCH / "definitely-not-here.csv"),
        "properties": {"directory": str(_SCRATCH)},
        "profile": {"behaviour": "file_reader"}})

    assert result["reachable"] is False, result
    assert "does not contain" in result["detail"]


def test_endpoints_are_parsed_out_of_real_connection_strings():
    """These shapes come from the cluster capture."""
    cases = [
        ("jdbc:oracle:thin:@ora-prod-01:1521/ORCL", ("ora-prod-01", 1521)),
        ("jdbc:postgresql://db.example.com:5432/sales", ("db.example.com", 5432)),
        ("jdbc:oracle:thin:@ora-prod-01/ORCL", ("ora-prod-01", 1521)),
        ("kafka-1.example.com:9092,kafka-2.example.com:9092",
         ("kafka-1.example.com", 9092)),
        ("https://acme.my.salesforce.com", ("acme.my.salesforce.com", 443)),
    ]
    for raw, expected in cases:
        assert probes._parse_endpoint(raw) == expected, raw


def test_an_unprobeable_connector_says_so():
    result = probes.check_dependency({
        "component": "ns.App_MysterySource",
        "properties": {"someSetting": "value"},
        "profile": {"behaviour": "unknown"}})
    assert result["reachable"] is None
    assert "cannot be probed" in result["detail"]


def test_a_component_that_could_not_be_read_is_not_reported_as_down():
    result = probes.check_dependency({
        "component": "ns.App_Source", "error": "KeyError: no canned response"})
    assert result["reachable"] is None
    assert "nothing to probe" in result["detail"]


# ================================================= PARSING AND FORMAT

def test_verdict_parsing_survives_fences_and_prose():
    fenced = "```json\n" + json.dumps(verdict()) + "\n```"
    parsed, problem = prompts.parse_verdict(fenced)
    assert problem is None and parsed["headline"]

    chatty = "Here is what I found:\n" + json.dumps(verdict()) + "\nHope that helps."
    parsed, problem = prompts.parse_verdict(chatty)
    assert problem is None and parsed["headline"]

    for rubbish in ("", "not json at all", "{", "[1,2,3]"):
        parsed, problem = prompts.parse_verdict(rubbish)
        assert parsed is None and problem, rubbish


def test_unrecognised_confidence_becomes_low():
    parsed, _ = prompts.parse_verdict(json.dumps(verdict(confidence="VERY")))
    assert parsed["confidence"] == "low"


def test_the_message_says_what_broke_then_what_i_did_then_what_you_must_do():
    """The three shapes from TASK-4, and nothing else on the first screen."""
    fixed = slack_io.compose_incident(
        REJECTING, verdict(what_happened="The target refused four duplicate rows."),
        {"shape": "fixed",
         "did": ["deleted 4 duplicate rows blocking the resume",
                 "resumed the pipeline"],
         "result": "1,204 rows written, checkpoint advanced, running normally"})

    assert fixed.startswith(slack_io.RESOLVED)
    assert "— fixed" in fixed
    assert "I did:  deleted 4 duplicate rows blocking the resume, then " \
           "resumed the pipeline" in fixed
    assert "Result: 1,204 rows written" in fixed

    command = slack_io.compose_incident(
        REJECTING, verdict(what_happened='The writer expects a "city" column '
                                         'that no longer exists.'),
        {"shape": "needs_command",
         "why_i_cannot": "Everything else is ready. I cannot execute DDL.",
         "command": {"sql": 'ALTER TABLE "public"."target" ADD COLUMN "city" text;',
                     "where": "neondb (the target)",
                     "derived_from": "Derived from the source schema.",
                     "next": "I will resume and verify as soon as it exists.",
                     "reversal": 'ALTER TABLE "public"."target" DROP COLUMN "city";'}})

    # The fault is the headline here too. The ask is carried by the 🙋
    # and by the "Run this on" block, not by a label repeating the shape.
    assert command.startswith(slack_io.NEEDS_HUMAN)
    assert "Source file went missing" in command.splitlines()[0]
    assert "Run this on neondb (the target):" in command
    assert 'ALTER TABLE "public"."target" ADD COLUMN "city" text;' in command
    assert "I will resume and verify" in command
    assert "To undo it:" in command

    blocked = slack_io.compose_incident(
        REJECTING, verdict(headline='Replication slot "striim_slot" is gone',
                           what_happened="Change capture has no position to "
                                         "resume from."),
        {"shape": "blocked",
         "blocked_reason": "there is no position to resume from",
         "next_action": "recreate the slot and run a fresh initial load"})

    # The headline NAMES THE FAULT. The decision is the emoji and the
    # "Not restarting" line - it is not the title.
    assert blocked.startswith(slack_io.RAISED)
    assert 'Replication slot "striim_slot" is gone' in blocked.splitlines()[0]
    assert "stopped, not safe to restart" not in blocked
    assert "Not restarting: there is no position to resume from" in blocked
    assert "You need to: recreate the slot" in blocked


def test_two_different_faults_do_not_get_the_same_headline():
    """Both live incidents arrived as "stopped, not safe to restart" - the
    same subject line for a dropped slot and a missing file, and the one
    thing telling them apart was computed and then thrown away."""
    record = {"shape": "blocked", "blocked_reason": "r", "next_action": "a"}

    slot = slack_io.compose_incident(
        "ns.A", verdict(headline='Replication slot "striim_slot" is gone'),
        record)
    file_gone = slack_io.compose_incident(
        "ns.B", verdict(headline="Source file AI.csv went missing"), record)

    assert slot.splitlines()[0] != file_gone.splitlines()[0]
    assert "striim_slot" in slot.splitlines()[0]
    assert "AI.csv" in file_gone.splitlines()[0]


def test_the_machine_noise_is_off_the_parent_message():
    """confidence · evidence · zone reads like a debug line, because it is."""
    text = slack_io.compose_incident(
        TRIAL5, verdict(), {"shape": "blocked",
                            "blocked_reason": "no usable position"})

    for noise in ("confidence", "zone", "couldn't check", "medium", "target"):
        assert noise not in text.lower(), (noise, text)

    # It is all still available, one tap away.
    detail = slack_io.compose_detail(
        {"verdict": verdict(), "rounds": 2, "trace": ["a", "b", "c"]})
    assert "high confidence" in detail
    assert "source zone" in detail
    assert "2 rounds" in detail and "3 checks" in detail


# ============================================ THE THREAD-REPLY STANDARD
#
# Three replies, and only three kinds. Everything else lives in the
# terminal and the audit log. These test the rule rather than any one
# caller, because the rule is only worth having if it cannot be forgotten
# by the next person who adds a reply.

def test_only_a_state_change_reaches_a_thread():
    notifier = Recorder()
    thread = notifier.post_incident("🔴 test")

    for kind in ("action", "outcome", "failure"):
        Recorder.reply(notifier, thread, f"a {kind}", kind=kind)
    assert len(notifier.replies) == 3, notifier.replies

    fresh = Recorder()
    other = fresh.post_incident("🔴 test")
    for noise in ("high confidence", "3 checks in 2 rounds",
                  "considered dedupe_target_rows, but there are no keys",
                  "checkpoint written at 14:02:11",
                  "2 other pipelines share this target"):
        Recorder.reply(fresh, other, noise)

    assert fresh.replies == [], fresh.replies
    assert len(fresh.withheld) == 5, fresh.withheld


def test_a_thread_never_holds_more_than_three_replies():
    """The fourth is real news and still does not go in - the thread is
    not where a person should be reading anyway."""
    notifier = Recorder()
    thread = notifier.post_incident("🔴 test")

    for n in range(6):
        Recorder.reply(notifier, thread, f"outcome {n}", kind="outcome")

    assert notifier.replies == ["outcome 0", "outcome 1", "outcome 2"], \
        notifier.replies
    assert len(notifier.withheld) == 3, notifier.withheld


def test_a_real_incident_leaves_a_thread_a_person_can_ignore():
    """End to end: whatever the run does, the thread stays inside the rule."""
    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="", human_action_needed="",
                 likely_cause="the source API refused a call and it halted")
    # Its own ledger: the shared one would see this exact fault a second
    # time in the same hour and refuse the restart, which is correct
    # behaviour and the wrong thing to be testing here.
    _, notifier, _ = run("staged_restart_fails.json", model=FakeModel(ok),
                         namespace="jobregonr",
                         ledger=act.Ledger(path=_SCRATCH / "l-thread-rule.json"))

    assert len(notifier.replies) <= 3, notifier.replies
    for reply in notifier.replies:
        lowered = reply.lower()
        for banned in ("confidence", "rounds", "checks in", "considered ",
                       "other pipeline", "checkpoint"):
            assert banned not in lowered, (banned, reply)


def test_the_parent_carries_the_action_without_the_thread():
    """The bar: act on it without opening the thread, without a follow-up."""
    ok = verdict(fault_zone="pipeline", restart_would_help=True,
                 confidence="high", why_not="", human_action_needed="",
                 likely_cause="the source API refused a call and it halted")
    # Its own ledger: the shared one would see this exact fault a second
    # time in the same hour and refuse the restart, which is correct
    # behaviour and the wrong thing to be testing here.
    _, notifier, _ = run("staged_restart_fails.json", model=FakeModel(ok),
                         namespace="jobregonr",
                         ledger=act.Ledger(path=_SCRATCH / "l-parent-alone.json"))

    parent = notifier.final
    assert "salesforce2bigquery" in parent          # which pipeline
    assert "resumed the pipeline" in parent         # what the agent did
    assert "did not hold" in parent                 # how that went
    assert "source API refused a call" in parent    # why it is still broken
    assert "needs a person" in parent               # what happens next


def test_a_dry_run_says_so_on_the_parent_and_hands_over_the_statement():
    """Otherwise it reads as an agent that saw the fault and did nothing."""
    text = slack_io.compose_incident(
        TRIAL5, verdict(),
        nodes.build_record({"app": TRIAL5, "tier": 1, "did": [],
                            "dry_run_statement": "RESUME sarah_moolai.Trial5;",
                            "reason": "the dependency is back",
                            "verdict": verdict(), "evidence": {}}))

    assert "dry-run mode" in text, text
    assert "RESUME sarah_moolai.Trial5;" in text, text
    assert "STRIIM_LIVE=1" in text, text
    assert "changed nothing" in text, text


def test_a_database_that_will_not_open_names_itself_and_the_fix():
    """The observed failure: the agent knew the host, the database name and
    the server's own reason, and said "a person should look at this"."""

    def message(error):
        db._DISCOVERED["target"] = db.Endpoint(
            role="target", host="ep-x.neon.tech", port=5432,
            database="striim_target", user="neondb_owner", password="x",
            component="TargetWriter",
            password_from="STRIIM_DBPASS_EP_X_NEON_TECH")
        try:
            return slack_io.compose_incident(TRIAL5, verdict(), nodes.build_record({
                "app": TRIAL5, "tier": 3, "did": [], "fault_zone": "target",
                "reason": "the target database cannot be reached",
                "verdict": verdict(human_action_needed="", restart_would_help=False),
                "evidence": {"target_db": {"configured": True,
                                           "reachable": False, "error": error}}}))
        finally:
            db.forget()

    gone = message('OperationalError: FATAL:  database "striim_target" does not exist')
    assert "striim_target" in gone and "ep-x.neon.tech" in gone, gone
    assert "does not exist" in gone, gone
    assert "rename it back" in gone and "point the target component" in gone, gone
    assert "TargetWriter" in gone, gone
    assert "a person should look at this" not in gone, gone

    # A rotated password is a different fix, and it names the variable.
    denied = message('OperationalError: FATAL:  password authentication failed')
    assert "STRIIM_DBPASS_EP_X_NEON_TECH" in denied, denied

    # Anything else still carries the server's own words rather than a shrug.
    other = message('OperationalError: could not translate host name "ep-x" to address')
    assert "could not translate host name" in other, other
    assert "a person should look at this" not in other, other


def test_a_vanished_database_is_never_claimed_as_repairable():
    """It is outside the DDL contract on purpose - no CREATE DATABASE, and
    no write path into Striim's component configuration."""
    assert "create_database" not in skills.SKILLS
    for skill in skills.SKILLS.values():
        assert skill.zone in ("pipeline", "target", "source"), skill.name

    for statement in ('CREATE DATABASE "striim_target"',
                      'ALTER DATABASE "old" RENAME TO "new"',
                      'DROP DATABASE "striim_target"'):
        found = ddl.classify(statement)
        assert found["kind"] == ddl.REFUSED, (statement, found)
        assert found["refusals"], statement


def test_the_tier_logic_never_narrates_itself():
    """Observed live: "Not restarting: confidence is medium — a person
    should approve this one". That is the machinery talking about itself.
    The reader wants what is wrong and what to do."""
    services = services_for("trial5_running.json")
    unsure = verdict(fault_zone="pipeline", restart_would_help=True,
                     confidence="medium", why_not="", human_action_needed="",
                     likely_cause="the source API refused a call",
                     missing_information="the API's own error text was "
                                         "never published by this cluster")

    decided = nodes.decide(
        {"app": TRIAL5, "status": "HALT", "verdict": unsure, "evidence": {},
         "fingerprint": "f", "did": []}, services)

    assert decided["tier"] == 2, decided
    assert "confidence" not in decided["reason"].lower(), decided["reason"]
    assert "the API's own error text" in decided["reason"], decided["reason"]

    # It is still recorded - just not in the reader's face.
    assert "medium" in decided.get("why_unsure", ""), decided


def test_no_message_ever_contains_a_word_from_the_machinery():
    """tier, confidence, rounds, checks, skills - terminal and audit only."""
    machine = ("tier", "confidence", "round", "skill", "precondition",
               "guardrail", "verdict", "fingerprint", "zone")

    messages = [
        slack_io.compose_incident(TRIAL5, verdict(), nodes.build_record({
            "app": TRIAL5, "tier": 2, "did": [], "reason": r,
            "verdict": verdict(), "evidence": {}}))
        for r in ("the source API refused a call and it halted",
                  "the target is missing the city column")]

    for text in messages:
        for word in machine:
            assert word not in text.lower(), (word, text)


def test_it_never_tells_a_human_to_do_what_it_just_refused():
    """Observed live: declined to restart, then said "Restart the source
    component to reconnect to striim_slot". Both cannot be right."""
    refused = {"app": TRIAL5, "tier": 3, "did": [],
               "reason": "not restarting — the replication slot is gone",
               "fault_zone": "source",
               "evidence": {"source_config": {"expects": "striim_slot"}},
               "verdict": verdict(
                   restart_would_help=False,
                   human_action_needed="Restart the source component to "
                                       "reconnect to striim_slot")}

    action = nodes._human_action(refused)
    assert action, action
    assert "restart the source component" not in action.lower(), action

    text = slack_io.compose_incident(TRIAL5, refused["verdict"],
                                     nodes.build_record(refused))
    assert "Restart the source component" not in text, text

    # But where nothing was refused, the model's action is left alone.
    allowed = {**refused, "reason": "the file is gone",
               "verdict": verdict(restart_would_help=None,
                                  human_action_needed="Restart the source "
                                                      "component once the "
                                                      "file is back")}
    assert "Restart the source component" in nodes._human_action(allowed)


def test_no_line_of_a_message_is_ever_printed_twice():
    """The observed fault: two slots, same sentence, both useless."""
    repeated = "Inspect exception store rows to find rejection cause"
    text = slack_io.compose_incident(
        TRIAL5, verdict(what_happened=repeated),
        {"shape": "blocked", "blocked_reason": repeated,
         "next_action": repeated})

    body = [line.strip() for line in text.splitlines() if line.strip()]
    normalised = ["".join(c for c in line.lower() if c.isalnum())
                  for line in body]
    assert len(normalised) == len(set(normalised)), body


def test_a_healthy_pipeline_gets_no_restart_advice():
    """Warning about a fix nobody proposed is noise, and noise gets muted."""
    healthy = verdict(healthy=True, why_not="", human_action_needed="")
    text = slack_io.compose_incident(TRIAL5, healthy, {"shape": "unknown"})
    assert "Don't restart" not in text
    assert "You need to" not in text


def test_the_parent_is_provisional_then_rewritten_and_never_claims_early():
    """The thread must exist before the action, so the parent starts
    provisional. It is rewritten once the outcome is known.

    The provisional wording carries most of the weight: chat_update does
    not re-notify, so whoever got the push for the first version never gets
    one for the rewrite. It must never claim a fix that has not happened.
    """
    ok = verdict(healthy=False, fault_zone="pipeline",
                 headline="Poller stopped after an API error",
                 what_happened="The Salesforce reader halted and stayed down.",
                 likely_cause="a transient API failure",
                 restart_would_help=True, confidence="high", why_not="",
                 human_action_needed="", suggested_tier=1)

    final, notifier, services = run(
        "staged_safe_restart.json", model=FakeModel(ok), namespace="jobregonr",
        ledger=act.Ledger(path=_SCRATCH / "ledger-provisional.json"))

    provisional = notifier.posts[0]
    assert provisional.startswith(slack_io.ACTING), provisional
    assert "working on it" in provisional
    for claim in ("fixed", "recovered", "resolved"):
        assert claim not in provisional.lower(), (claim, provisional)

    assert notifier.rewrites, "the parent was never rewritten"
    assert notifier.final.startswith(slack_io.RESOLVED), notifier.final
    assert "— fixed" in notifier.final
    assert "I did:  resumed the pipeline" in notifier.final, notifier.final
    assert final["final_shape"] == "fixed"

    # CLOSE owns the open-incident book, and a fixed incident leaves it.
    assert SALESFORCE not in services.open_incidents


def test_a_repair_that_ran_is_verified_even_if_the_resume_was_refused():
    """The regression: a write happened, was never checked, and the thread
    said only "refused".

    With the row-count gate gone this stops being theoretical - the repair
    can be arbitrarily large, and reporting only the refusal would leave a
    changed customer database entirely unaccounted for.
    """
    # A ledger of its own - spending the budget here must not leak into
    # any other test that uses this fixture.
    spent = act.Ledger(path=_SCRATCH / "ledger-refused-resume.json")
    spent.clear()
    services = services_for("staged_safe_restart.json", ledger=spent)
    for n in range(config.MAX_RESTARTS_PER_HOUR):
        spent.record(SALESFORCE, "RESUME", f"other-{n}", live=False)

    state = {"app": SALESFORCE, "slack_thread_ts": None,
             "fingerprint": "fp-1",
             "repair_plan": {"will_execute": True,
                             "sql": "DELETE FROM orders WHERE batch_id = 'b'",
                             "rows_affected": 4, "incident": "i", "cause": "c"}}

    original = config.NAMESPACE
    config.NAMESPACE = "jobregonr"
    try:
        with sqlite_backed(rows=[(i, "b", "x") for i in range(4)]):
            out = nodes.act_node(state, services)
    finally:
        config.NAMESPACE = original

    # The repair is recorded and the refusal is reported - not one or the
    # other.
    assert out["action_result"]["repaired"] is True, out
    assert "refused" in out["action_result"], out
    assert any("changed 4 rows" in d for d in out["did"]), out["did"]

    # ...and verification is NOT skipped, because a write really happened.
    assert nodes.verify_node({**state, **out, "verification": None},
                             services)["verification"] is not None


def test_a_lookup_request_never_becomes_the_next_action():
    """TASK-4 fault 2. The agent must not delegate its own reads."""
    for asked in ("Inspect exception store rows to find rejection cause",
                  "Check the target database for duplicates",
                  "Review the rejected rows",
                  "Investigate the source system"):
        assert nodes._is_a_lookup(asked), asked

    for real in ("restore AI.csv to UploadedFiles/sarah_moolai/",
                 "recreate the slot and run a fresh initial load",
                 "run the statement above on the target"):
        assert not nodes._is_a_lookup(real), real

    # And it is dropped in favour of something computed.
    state = {"tier": 3, "fault_zone": "source",
             "verdict": {"human_action_needed": "Inspect exception store rows"},
             "evidence": {"source_config":
                          {"expects": "UploadedFiles/sarah_moolai/AI.csv"}}}
    action = nodes._human_action(state)
    assert "AI.csv" in action and "Inspect" not in action, action


# ============================================ REGISTRY AND SLACK COMMANDS

def test_the_registry_refuses_names_it_could_never_act_on():
    registry = Registry(path=_SCRATCH / "reg.json", namespace="sarah_moolai")
    registry.replace([])

    ok, message = registry.add(ORACLE)
    assert ok is False
    assert f"diagnose {ORACLE}" in message

    ok, message = registry.add("Trial5")
    assert ok is False and "no namespace" in message

    ok, _ = registry.add(TRIAL5)
    assert ok and TRIAL5 in registry


def test_the_registry_survives_a_restart():
    path = _SCRATCH / "reg-persist.json"
    first = Registry(path=path, namespace="sarah_moolai")
    first.replace([TRIAL5])

    second = Registry(path=path, namespace="sarah_moolai")
    assert second.all() == [TRIAL5]


def test_slack_commands_route_to_the_supervisor():
    services = services_for("trial5_running.json")
    registry = Registry(path=_SCRATCH / "reg-slack.json",
                        namespace="sarah_moolai")
    registry.replace([])
    supervisor = Supervisor(services, registry)
    console = slack_io.Console(supervisor, bot_token=None, app_token=None)

    assert "Now monitoring" in console.handle(f"monitor {TRIAL5}")
    assert TRIAL5 in console.handle("status")
    assert "diagnose" in console.handle(f"monitor {ORACLE}")
    assert "Stopped monitoring" in console.handle(f"unmonitor {TRIAL5}")
    assert console.handle("help").startswith("*Striim SRE agent*")


def test_every_command_answers_immediately():
    """A console that goes quiet for a minute reads as broken on stage."""
    import time as clock

    services = services_for("trial5_halt.json")
    registry = Registry(path=_SCRATCH / "reg-ack.json",
                        namespace="sarah_moolai")
    registry.replace([])
    supervisor = Supervisor(services, registry)
    console = slack_io.Console(supervisor, bot_token=None, app_token=None)

    for command in (f"monitor {TRIAL5}", "status", f"diagnose {ORACLE}",
                    f"unmonitor {TRIAL5}", "help"):
        started = clock.monotonic()
        answer = console.handle(command)
        elapsed = clock.monotonic() - started
        assert answer, command
        assert elapsed < 1.0, (command, elapsed)


def test_diagnose_says_up_front_that_it_will_not_act_elsewhere():
    """The read/write asymmetry is worth pointing at, not hiding."""
    services = services_for("real_oracle_cdc_terminated.json")
    registry = Registry(path=_SCRATCH / "reg-asym.json",
                        namespace="sarah_moolai")
    registry.replace([])
    console = slack_io.Console(Supervisor(services, registry),
                               bot_token=None, app_token=None)

    answer = console.handle(f"diagnose {ORACLE}")
    assert "refuse to act" in answer, answer
    assert "read it in full" in answer, answer


def test_the_cli_waits_for_a_diagnosis_instead_of_backgrounding_it():
    """A one-shot process would exit before a background thread ran."""
    services = services_for("trial5_running.json")
    registry = Registry(path=_SCRATCH / "reg-sync.json",
                        namespace="sarah_moolai")
    registry.replace([])
    supervisor = Supervisor(services, registry)

    answer = supervisor.diagnose(TRIAL5, background=False)
    assert "Looking at" not in answer, answer
    assert TRIAL5 in answer, answer


def test_an_unrecognised_command_gets_help_not_silence():
    services = services_for("trial5_running.json")
    registry = Registry(path=_SCRATCH / "reg-help.json",
                        namespace="sarah_moolai")
    registry.replace([])
    console = slack_io.Console(Supervisor(services, registry),
                               bot_token=None, app_token=None)

    assert console.handle("monitor").startswith("`monitor` needs")
    assert console.handle("what pipelines are you watching") == slack_io.HELP
    assert console.handle("agent are you there") == slack_io.HELP

    # ...but ordinary channel chatter is left alone.
    assert console.handle("morning all") is None
    assert console.handle("lunch?") is None


def test_a_human_marking_it_resolved_stands_the_agent_down():
    """Do not fight a person who has said they have got this."""
    notifier = Recorder()
    final, _, services = run("trial5_halt.json", notifier=notifier)
    thread = final["slack_thread_ts"]
    assert services.open_incidents[TRIAL5]["thread"] == thread

    registry = Registry(path=_SCRATCH / "reg-close.json",
                        namespace="sarah_moolai")
    supervisor = Supervisor(services, registry)
    assert supervisor.human_closed(thread) == TRIAL5

    # Nothing more goes into that thread, not even an allowed kind.
    before = len(notifier.replies)
    notifier.reply(thread, "one more thing", kind="outcome")
    assert len(notifier.replies) == before, notifier.replies
    assert notifier.is_closed(thread)

    # And the next poll of the same fault raises nothing new.
    services.striim.invalidate()
    again = build_graph(services).invoke(new_incident(TRIAL5))
    assert again["silent"] is True
    assert "standing down" in again["reason"], again["reason"]


def test_the_state_machine_moves_one_reaction_at_a_time():
    """Never simultaneously 'being worked on' and 'resolved'."""
    calls = []

    class Tracking(slack_io.Notifier):
        def __init__(self):
            super().__init__(token=None, channel="#test", echo=False)
            outer = calls

            class Client:
                def chat_postMessage(self, **kwargs):
                    return {"ts": "111.222", "channel": "C1"}

                def chat_update(self, **kwargs):
                    return {}

                def reactions_add(self, **kwargs):
                    outer.append(("add", kwargs["name"]))

                def reactions_remove(self, **kwargs):
                    outer.append(("remove", kwargs["name"]))

            self.client = Client()

    notifier = Tracking()
    handle = notifier.post_incident(f"{slack_io.RAISED}  *Trial5* — broke")
    notifier.set_state(handle, slack_io.ACTING)
    notifier.set_state(handle, slack_io.RESOLVED)

    assert calls == [("add", "red_circle"),
                     ("remove", "red_circle"), ("add", "wrench"),
                     ("remove", "wrench"), ("add", "white_check_mark")], calls


# ================================================================ RUN

def main():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print(f"\n{len(tests)} acceptance tests, all offline, all against real "
          f"cluster captures\n")
    failures = []

    for name, test in tests:
        try:
            test()
            print(f"  PASS  {name[5:].replace('_', ' ')}")
        except AssertionError as error:
            failures.append((name, error))
            print(f"  FAIL  {name[5:].replace('_', ' ')}\n        {error}")
        except Exception as error:
            failures.append((name, error))
            print(f"  ERROR {name[5:].replace('_', ' ')}\n"
                  f"        {type(error).__name__}: {error}")

    print()
    if failures:
        print(f"{len(failures)} of {len(tests)} failed.")
    else:
        print(f"All {len(tests)} passed.")

    shutil.rmtree(_SCRATCH, ignore_errors=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
