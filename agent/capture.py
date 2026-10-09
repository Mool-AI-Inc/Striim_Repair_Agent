"""
Fixture capture
===============

Watches a pipeline and, the moment it sees a fault, writes the COMPLETE
health record to a fixture file - everything, unfiltered, so fields nobody
thought mattered are still there next month when they turn out to.

Run this, then break the pipeline, and leave it broken for a couple of
minutes (/health lags, so an instant capture catches the old world).

    python -m agent capture sarah_moolai.Trial5

Offline fixtures are not a nicety. Without them every test run needs a real
pipeline broken in a specific way first, which means the tests get run
once. With them the whole acceptance suite runs in under a second on a
train, and the demo can be rehearsed without touching the cluster.

Timestamps are rewritten to {"$ago_ms": N} on the way out so a fixture
still means "this broke 90 seconds ago" when it is loaded next year.
"""

import json
import time
from datetime import datetime
from pathlib import Path

from . import config
from . import striim_tools as st

TIME_FIELDS = {"lastModifiedTime", "lastEventTime", "lastWriteTime",
               "timestamp", "startTime", "endTime", "created"}


def relativise(node, now_ms):
    """Turn absolute epoch-millisecond timestamps into "N ms ago"."""
    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            if key in TIME_FIELDS and isinstance(value, (int, float)) and value > 1e12:
                out[key] = {"$ago_ms": round(now_ms - value)}
            else:
                out[key] = relativise(value, now_ms)
        return out
    if isinstance(node, list):
        return [relativise(item, now_ms) for item in node]
    return node


def capture(striim, app, label=None, directory=None):
    """Write one fixture from the current health record."""
    striim.invalidate()
    record = striim.health()

    fixture = {
        "captured_from": app,
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "health": relativise(record, time.time() * 1000),
        "console": {},
    }

    # The component configuration is a separate endpoint, so grab it too -
    # it is where the read position lives, and the read position is the
    # difference between "restart it" and "do not restart it".
    flow = st.get_flow_stats(striim, app)
    for end in ("source", "target"):
        component = (flow.get(end) or {}).get("component")
        if not component:
            continue
        statement = f"DESCRIBE {component};"
        try:
            fixture["console"][statement] = st.console(striim, statement)
        except Exception as error:
            fixture["console"][statement] = {"_capture_error": str(error)}

    status = record.get("appHealthMap", {}).get(app, {}).get("status", "unknown")
    name = label or f"{app.split('.')[-1].lower()}_{status.lower()}"
    directory = Path(directory or "tests/fixtures")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.json"

    with open(path, "w") as handle:
        json.dump(fixture, handle, indent=2)

    return path, status


def watch_and_capture(striim, app, directory=None, poll=None):
    """Sit on a pipeline and capture every distinct state it passes through."""
    poll = config.POLL_SECONDS if poll is None else poll
    seen = set()

    print(f"Watching {app}. Break it now — every new state gets saved.\n")

    while True:
        try:
            striim.invalidate()
            status = st.get_status(striim, app)["status"]
        except Exception as error:
            print(f"  fetch failed: {error}")
            time.sleep(poll)
            continue

        clock = datetime.now().strftime("%H:%M:%S")

        if status not in seen:
            seen.add(status)
            path, _ = capture(striim, app, directory=directory)
            print(f"[{clock}] {status}  ->  saved {path}")
        else:
            print(f"[{clock}] {status}")

        time.sleep(poll)
