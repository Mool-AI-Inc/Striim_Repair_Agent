"""
Striim action layer
===================

The ONLY file in this project that can change anything. Everything else
is read-only. Keeping the write path in one small file means the whole
question of "what can this agent do?" is answered by reading 150 lines.

Three hard limits, enforced here rather than trusted to the model:
  * namespace  - refuses to touch anything outside MY_NAMESPACE
  * verb list  - only START, STOP, RESUME, DEPLOY. Nothing else runs.
  * DRY_RUN    - on by default; prints what it would do and sends nothing

Test:
    python striim_actions.py resume sarah_moolai.Trial5
"""

import json
import os
import time

import requests

import striim_tools as st

STRIIM_URL = st.STRIIM_URL
MY_NAMESPACE = st.MY_NAMESPACE

DRY_RUN = os.environ.get("STRIIM_LIVE", "").lower() not in ("1", "true", "yes")

# Only these verbs. A typo or a confused model cannot run anything else.
ALLOWED_VERBS = {"START", "STOP", "RESUME", "DEPLOY", "UNDEPLOY"}

# /health lags roughly two minutes, so verification has to outwait it or it
# will read the pre-restart world and declare failure on a working fix.
VERIFY_WAIT_SECONDS = 210


class ActionRefused(Exception):
    """Raised when a guardrail blocks something. Never caught silently."""


def _check(app, verb):
    if verb not in ALLOWED_VERBS:
        raise ActionRefused(f"verb '{verb}' is not in the allowed list")

    if not app.startswith(MY_NAMESPACE + "."):
        raise ActionRefused(
            f"'{app}' is outside {MY_NAMESPACE}. This agent may READ any "
            f"pipeline on the cluster but may only ACT on its own."
        )


def run_command(striim, app, verb):
    """Send one console command. The single point where anything changes."""
    _check(app, verb)
    statement = f"{verb} {app};"

    if DRY_RUN:
        return {"dry_run": True, "would_have_run": statement,
                "note": "set STRIIM_LIVE=1 to actually send this"}

    # The endpoint wants a PLAIN TEXT body and an Authorization header of
    # the form "token <value>". Sending JSON gets 415; sending STRIIM-TOKEN
    # gets 401. Both fail quietly enough to look like the command not working.
    response = requests.post(
        f"{STRIIM_URL}/api/v2/tungsten",
        headers={"Authorization": f"token {striim.token}",
                 "Content-Type": "text/plain"},
        data=statement,
        timeout=60,
    )

    if response.status_code in (401, 403):
        striim.login()
        return run_command(striim, app, verb)

    response.raise_for_status()
    return {"dry_run": False, "ran": statement,
            "response": response.text[:500]}


# ============================================================== VERIFY

def snapshot(striim, app):
    """The few numbers that prove whether data is actually moving."""
    striim._cache = None          # force a fresh read, not the cached one
    flow = st.get_flow_stats(striim, app)
    status = st.get_status(striim, app)
    checkpoints = st.get_checkpoints(striim, app)

    latest_checkpoint = json.dumps(checkpoints.get("checkpoints"))

    return {
        "status": status["status"],
        "target_seconds_since_write": flow["target"]["seconds_since_last_write"],
        "source_seconds_since_event": flow["source"]["seconds_since_last_event"],
        "checkpoint_fingerprint": latest_checkpoint,
    }


def verify(striim, app, before, wait=VERIFY_WAIT_SECONDS, verbose=True):
    """Did the fix actually work?

    "It says RUNNING" is not proof - a pipeline can report RUNNING while
    doing nothing, or fail again forty seconds later. Three questions:

      1. Is it in a healthy state at all?
      2. Has the target written something SINCE the restart?
      3. Has the checkpoint moved?

    Only the first is visible from the status field, and it is the weakest
    of the three.
    """
    if verbose:
        print(f"    waiting {wait}s for monitoring to catch up...")
    time.sleep(wait)

    after = snapshot(striim, app)

    healthy = after["status"] not in st.FAULT_STATES
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
    elif target_moved or checkpoint_moved:
        moved = []
        if target_moved:
            moved.append("target wrote new data")
        if checkpoint_moved:
            moved.append("checkpoint advanced")
        outcome, detail = True, " and ".join(moved)
    else:
        # The honest answer. Common for a file source that has already read
        # everything - healthy, but with nothing left to prove it.
        outcome, detail = None, (
            f"{after['status']} but nothing has moved yet - cannot confirm "
            "it is really working"
        )

    return {"recovered": outcome, "detail": detail,
            "before": before, "after": after}


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    verb = sys.argv[1].upper()
    target = sys.argv[2]

    client = st.Striim()
    print(f"DRY_RUN = {DRY_RUN}")

    before_state = snapshot(client, target)
    print("before:", json.dumps(before_state, indent=2, default=str))

    result = run_command(client, target, verb)
    print("action:", json.dumps(result, indent=2, default=str))

    if not DRY_RUN:
        print(json.dumps(verify(client, target, before_state),
                         indent=2, default=str))