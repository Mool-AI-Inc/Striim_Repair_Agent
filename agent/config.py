"""
Every knob in one place
=======================

Nothing else in the package reads os.environ. When someone asks "what does
this agent do at 3am and why did it wait four minutes", the answer is here
and it is one screen long.

The timing constants are not arbitrary. Each one is a workaround for a
measured Striim behaviour - see FINDINGS.md. Shortening them does not make
the agent faster, it makes it wrong.
"""

import os
from pathlib import Path


def _flag(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _number(name, default):
    try:
        return int(os.environ[name])
    except (KeyError, ValueError, TypeError):
        return default


# ------------------------------------------------------------ STRIIM
STRIIM_URL = os.environ.get("STRIIM_URL", "https://developer.striim.com")

# The one namespace this agent may WRITE to. Reads are cluster-wide on
# purpose: diagnosing someone else's Oracle pipeline is harmless, and it is
# how we demonstrate that this is not a file-reader-specific toy.
NAMESPACE = os.environ.get("STRIIM_NAMESPACE", "sarah_moolai")

# Off by default, always. Every write path checks this.
LIVE = _flag("STRIIM_LIVE")
DRY_RUN = not LIVE


# ------------------------------------------------------------ TIMING
#
# /health is a cached cluster snapshot that lags roughly two minutes. It is
# not a live view. Three constants fall out of that single fact.

# How often WATCH polls. Faster than this buys nothing - the snapshot
# underneath is not refreshing any quicker.
POLL_SECONDS = _number("STRIIM_POLL_SECONDS", 60)

# Verification must OUTWAIT the lag or it reads the pre-restart world and
# declares a working fix a failure. 210s, not 120s, because the lag is
# "roughly" two minutes and being early is the expensive direction to be wrong.
VERIFY_WAIT_SECONDS = _number("STRIIM_VERIFY_WAIT", 210)

# Error text is published separately from the status change, and later.
# Diagnosing a fault with no error text attached produces a confident
# diagnosis of nothing, so it is worth waiting a little.
#
# But only a little. These started at 4 x 45s, which is three minutes of
# dead air and was the largest single component of a seven-minute
# detection-to-incident time - on a cluster that publishes no error text at
# all, so the wait expired empty every single time. EVIDENCE also skips the
# wait entirely when the whole cluster's issuesList is empty, which is the
# common case here; these numbers only apply where text really does arrive
# late. Raise them for an instance that publishes properly.
EVIDENCE_WAIT_SECONDS = _number("STRIIM_EVIDENCE_WAIT", 20)
EVIDENCE_ATTEMPTS = _number("STRIIM_EVIDENCE_ATTEMPTS", 2)


# ------------------------------------------------------------- MODEL
MODEL = os.environ.get("STRIIM_AGENT_MODEL", "claude-sonnet-5")
MAX_TOKENS = _number("STRIIM_AGENT_MAX_TOKENS", 4000)

# The diagnosis loop is the model's; this stops it being infinite.
MAX_ROUNDS = _number("STRIIM_AGENT_MAX_ROUNDS", 8)


# --------------------------------------------------------- GUARDRAILS
# Enforced in Python, in two places: DECIDE picks a tier with them in mind,
# and the action layer refuses again on the way out. Defence in depth,
# because DECIDE is one refactor away from being wrong.
MAX_RESTARTS_PER_HOUR = _number("STRIIM_MAX_RESTARTS_PER_HOUR", 2)

# Corrective DML against the TARGET. Above this row count the agent stops
# and asks, no matter how confident it is. Conservative by default.
TARGET_MAX_ROWS = _number("TARGET_MAX_ROWS", 1000)

# Unset means the agent has no way to reach the target at all: it will
# propose corrective SQL in the Slack thread and execute nothing.
TARGET_DB_URL = os.environ.get("TARGET_DB_URL") or None

# The source is READ ONLY, always. This URL buys the agent the ability to
# check whether the cause has cleared and to read a replication slot - and
# nothing else. There is no write path for it anywhere in the package.
SOURCE_DB_URL = os.environ.get("SOURCE_DB_URL") or None

# A hung database must never stall the monitoring loop.
DB_CONNECT_TIMEOUT = _number("DB_CONNECT_TIMEOUT", 5)
DB_STATEMENT_TIMEOUT_MS = _number("DB_STATEMENT_TIMEOUT_MS", 15000)

# Rows per statement when filling a restored column back in. Not a limit on
# how much may be repaired - a backfill only ever writes where the target
# holds NULL, so it cannot destroy anything - but a bound on how much any
# single statement holds open.
BACKFILL_BATCH_ROWS = _number("BACKFILL_BATCH_ROWS", 2000)


# ------------------------------------------------------------- STALLS
#
# A pipeline can report RUNNING and move nothing. Observed live: RUNNING
# for 34 minutes, replication slot inactive, and ten consecutive
# checkpoints recording the identical LSN while their timestamps advanced.
# Every status check passed.

# How long both ends must be silent before that is worth investigating on a
# connector where silence is NOT normal.
STALL_SILENCE_SECONDS = _number("STRIIM_STALL_SILENCE", 600)

# How many observations of an unmoved position make a stall. Two could be
# one slow minute; this is deliberately several.
STALL_OBSERVATIONS = _number("STRIIM_STALL_OBSERVATIONS", 3)


# How long a destructive repair waits for somebody to reply `approve` in
# the thread before giving up and saying it has. A repair waiting forever
# is a silent failure.
APPROVAL_TIMEOUT_SECONDS = _number("STRIIM_APPROVAL_TIMEOUT", 900)


# ------------------------------------------------------------- DRIFT
#
# How often to compare each monitored pipeline's schemas, whether or not
# anything has broken. With schema evolution set to Ignore, the target
# falls behind the source silently and nothing fails until a write happens
# to need the missing column - so waiting for a fault to bring the agent
# here means the warning window has already gone.
#
# Not every poll: this costs two catalogue reads per pipeline.
SCHEMA_CHECK_SECONDS = _number("STRIIM_SCHEMA_CHECK", 600)

# What the dedupe and partial-batch skills need in order to bound a
# statement. All unset by default, and each skill DECLINES rather than
# guesses when its own is missing - there is no safe default for "which
# column identifies a duplicate".
TARGET_TABLE = os.environ.get("TARGET_TABLE") or None
TARGET_PRIMARY_KEY = os.environ.get("TARGET_PRIMARY_KEY") or None
TARGET_BATCH_COLUMN = os.environ.get("TARGET_BATCH_COLUMN") or None

# Above this share of throughput, a recurring exception is systemic rather
# than an isolated bad row - and proposing to ignore it would be proposing
# to discard real data. 1% by default.
IGNORABLE_MAX_RATIO = float(os.environ.get("IGNORABLE_MAX_RATIO", "0.01"))


# ------------------------------------------------------------- SLACK
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN") or None
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN") or None
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#striim-incidents")


# ------------------------------------------------------------- STATE
# Small JSON files, not a database. What is being watched, what has already
# been tried, and an append-only record of everything that was changed.
STATE_DIR = Path(os.environ.get("STRIIM_STATE_DIR", "state")).resolve()

REGISTRY_FILE = STATE_DIR / "registry.json"     # what am I watching
LEDGER_FILE = STATE_DIR / "ledger.json"         # what have I already tried
AUDIT_FILE = STATE_DIR / "audit.log"            # what did I change, ever


def ensure_state_dir():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR


def summary():
    """One line per setting, for the banner at startup. No secrets."""
    return {
        "striim_url": STRIIM_URL,
        "namespace": NAMESPACE,
        "mode": "LIVE" if LIVE else "DRY RUN",
        "model": MODEL,
        "slack": ("socket mode" if SLACK_BOT_TOKEN and SLACK_APP_TOKEN
                  else "outbound only" if SLACK_BOT_TOKEN
                  else "terminal only"),
        # NOT "not configured". Discovery is the normal path, so the
        # absence of an override says nothing about whether the agent can
        # reach a database - it reported "target db not configured" while
        # db_probe was answering successfully against that very database.
        # What the banner can honestly report is the MECHANISM; which host
        # resolved is per-pipeline and is printed when it resolves.
        "target_db": ("TARGET_DB_URL override" if TARGET_DB_URL
                      else "discovered per pipeline"),
        "source_db": ("SOURCE_DB_URL override (read-only)" if SOURCE_DB_URL
                      else "discovered per pipeline (read-only)"),
        "db_password": ("STRIIM_DBPASS_DEFAULT set"
                        if os.environ.get("STRIIM_DBPASS_DEFAULT")
                        else "per-host STRIIM_DBPASS_* only"),
        "poll_seconds": POLL_SECONDS,
        "verify_wait_seconds": VERIFY_WAIT_SECONDS,
    }
