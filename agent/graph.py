"""
The graph
=========

                    ┌──────────┐
                    │  WATCH   │
                    └────┬─────┘
                         │ fault detected
                         ▼
                  ┌─────────────┐   not a fault, or already open,
                  │   TRIAGE    │──────────────────────────────────▶ END
                  └──────┬──────┘   or somebody did it on purpose
                         ▼
                  ┌─────────────┐
                  │  EVIDENCE   │  wait for the error text, if it is coming
                  └──────┬──────┘
                         ▼
                  ┌─────────────┐
                  │  DIAGNOSE   │  tool-calling loop, the model drives
                  └──────┬──────┘
                         ▼
                  ┌─────────────┐
                  │   DECIDE    │  guardrails in code, not in the model
                  └──────┬──────┘
                         ▼
                  ┌─────────────┐
                  │  ESCALATE   │  post a PROVISIONAL incident, open a thread
                  └──┬───────┬──┘
             tier 1  │       │  tier 2 / 3
                     ▼       │
              ┌─────────────┐│
              │     ACT     ││ the only node that changes anything
              └──────┬──────┘│
                     ▼       │
              ┌─────────────┐│
              │   VERIFY    ││ did data actually MOVE?
              └──────┬──────┘│
                     ▼       ▼
                  ┌─────────────┐
                  │    CLOSE    │  rewrite the parent into its final form
                  └──────┬──────┘
                         └──────────────────────────────────────▶ END

Two differences from the sketch in PLAN.md, both deliberate.

ESCALATE runs BEFORE ACT rather than after it. The Slack thread has to
exist before the action and the verification can be posted as replies to
it, and it means someone watching sees "this broke, I am on it" straight
away rather than four minutes later when verification returns.

CLOSE then rewrites that parent into its final form. It is the only node
that decides what an incident finally says, the only one that sets the
final emoji, and the only one that updates the open-incident book - which
before it existed were three decisions spread across TRIAGE, ACT and
VERIFY, each handling them slightly differently.

Note what is NOT a node. "Has the cause actually cleared?" is gathered in
EVIDENCE with the rest of the baseline, not checked after the incident has
been posted - otherwise the parent announces "working on it" and then has
to retract it. It is a fact about the world, so it belongs with the facts.

The graph is per-incident. The loop back to WATCH lives in the supervisor
(cli.py), which invokes this graph once per app per poll. Keeping the loop
outside means each run has a clean state object you can print in full.
"""

from functools import partial

from langgraph.graph import END, StateGraph

from . import nodes
from .state import IncidentState


def build_graph(services):
    """Compile the graph around one set of services."""
    graph = StateGraph(IncidentState)

    graph.add_node("WATCH", partial(nodes.watch, services=services))
    graph.add_node("TRIAGE", partial(nodes.triage, services=services))
    graph.add_node("EVIDENCE", partial(nodes.evidence, services=services))
    graph.add_node("DIAGNOSE", partial(nodes.diagnose, services=services))
    graph.add_node("DECIDE", partial(nodes.decide, services=services))
    graph.add_node("ESCALATE", partial(nodes.escalate, services=services))
    graph.add_node("ACT", partial(nodes.act_node, services=services))
    graph.add_node("VERIFY", partial(nodes.verify_node, services=services))
    graph.add_node("CLOSE", partial(nodes.close_node, services=services))

    graph.set_entry_point("WATCH")
    graph.add_edge("WATCH", "TRIAGE")

    graph.add_conditional_edges("TRIAGE", _after_triage,
                                {"investigate": "EVIDENCE", "quiet": END})
    graph.add_edge("EVIDENCE", "DIAGNOSE")
    graph.add_edge("DIAGNOSE", "DECIDE")

    graph.add_conditional_edges("DECIDE", _after_decide,
                                {"report": "ESCALATE", "quiet": END})
    graph.add_conditional_edges("ESCALATE", _after_escalate,
                                {"act": "ACT", "done": "CLOSE"})

    graph.add_edge("ACT", "VERIFY")
    graph.add_edge("VERIFY", "CLOSE")
    graph.add_edge("CLOSE", END)

    return graph.compile()


def build_diagnose_graph(services):
    """The same graph without TRIAGE, for `diagnose <app>`.

    A one-off investigation is a person asking a direct question, so the
    "is this new? did somebody do it on purpose?" gate does not apply -
    they know, they are asking anyway. Everything downstream is identical,
    including every guardrail, which is the point of reusing the nodes
    rather than writing a second shorter path that drifts.
    """
    graph = StateGraph(IncidentState)

    graph.add_node("WATCH", partial(nodes.watch, services=services))
    graph.add_node("EVIDENCE", partial(nodes.evidence, services=services))
    graph.add_node("DIAGNOSE", partial(nodes.diagnose, services=services))
    graph.add_node("DECIDE", partial(nodes.decide, services=services))
    graph.add_node("ESCALATE", partial(nodes.escalate, services=services))
    graph.add_node("ACT", partial(nodes.act_node, services=services))
    graph.add_node("VERIFY", partial(nodes.verify_node, services=services))
    graph.add_node("CLOSE", partial(nodes.close_node, services=services))

    graph.set_entry_point("WATCH")
    graph.add_edge("WATCH", "EVIDENCE")
    graph.add_edge("EVIDENCE", "DIAGNOSE")
    graph.add_edge("DIAGNOSE", "DECIDE")
    graph.add_conditional_edges("DECIDE", _after_decide,
                                {"report": "ESCALATE", "quiet": END})
    graph.add_conditional_edges("ESCALATE", _after_escalate,
                                {"act": "ACT", "done": END})
    graph.add_edge("ACT", "VERIFY")
    graph.add_edge("VERIFY", END)

    return graph.compile()


def _after_triage(state):
    return "quiet" if state.get("silent") else "investigate"


def _after_decide(state):
    """Healthy pipelines say nothing. That is a feature, not an omission.

    An agent that posts "all clear" every minute trains people to ignore the
    channel, which costs more than it ever gives back.
    """
    return "quiet" if state.get("tier") is None else "report"


def _after_escalate(state):
    return "act" if state.get("tier") == 1 else "done"
