"""
Target repair - PLAN.md section 5, zone 2
=========================================

The pipeline sits between a source and a target, and the agent's rights are
not the same in each:

    the pipeline    full control      - fix and resume
    the target      DML allowed       - corrective SQL, then resume
    the source      READ ONLY         - never touch it, flag for a human

This file is zone 2 and nothing else. It is the only place a statement is
ever sent to a customer's database, and it is deliberately hard to use.

Five preconditions, all enforced below, none optional:

  1. Diagnose first.  There must be a named cause in the verdict that this
     statement specifically addresses. Never a guess.
  2. Count first.     Run the equivalent SELECT COUNT(*) and carry the
     number. "This will delete 412 rows" is the difference between an
     informed action and an accident.
  3. Show the SQL.    The exact statement goes into the Slack thread. There
     is no silent write, ever, at any row count.
  4. Bounded scope.   A WHERE clause tied to the diagnosed problem. No
     unqualified DELETE, no TRUNCATE, no DROP, one statement at a time.
  5. Log everything.  Statement, count, timestamp, incident - to the audit
     file, before and after.

Above TARGET_MAX_ROWS, or below high confidence, or where the row count
cannot be established, the answer is always the same: post the SQL and the
count to Slack and wait for a person.

A deliberate narrowing: CREATE TABLE and ALTER TABLE ADD COLUMN are
mentioned in the plan as useful corrective work, but they cannot satisfy
rule 4 - there is no WHERE clause on a DDL statement - and schema repair is
an explicit non-goal for this build. They are classified here as
propose-only, always. The agent will write the DDL out for a human; it will
not run it.
"""

import re

from . import config
from . import db
from .striim_actions import audit


class RepairRefused(Exception):
    """A guardrail blocked a target write. Always surfaced."""


# Statements the agent may execute unattended, given everything else passes.
EXECUTABLE_VERBS = {"DELETE", "UPDATE"}

# Statements it may write out for a human but must never run itself.
PROPOSE_ONLY_VERBS = {"CREATE", "ALTER", "INSERT", "MERGE"}

# Statements it will not even propose.
FORBIDDEN_VERBS = {"DROP", "TRUNCATE", "GRANT", "REVOKE", "SHUTDOWN",
                   "ALTER SYSTEM"}

_COMMENT = re.compile(r"(--[^\n]*)|(/\*.*?\*/)", re.S)
_DELETE = re.compile(r"^\s*DELETE\s+FROM\s+(?P<table>[\w.\"`\[\]]+)\s+"
                     r"WHERE\s+(?P<where>.+)$", re.I | re.S)
_UPDATE = re.compile(r"^\s*UPDATE\s+(?P<table>[\w.\"`\[\]]+)\s+SET\s+.+?\s+"
                     r"WHERE\s+(?P<where>.+)$", re.I | re.S)


def _strip_comments(sql):
    return _COMMENT.sub(" ", sql or "").strip()


def inspect(sql):
    """Take a proposed statement apart and say exactly what it is.

    Returns a dict. Never raises - the caller decides what to do with a
    statement that cannot be run, and "cannot be run" is a normal outcome
    that belongs in the Slack thread, not an exception.
    """
    cleaned = _strip_comments(sql).rstrip().rstrip(";").strip()
    upper = cleaned.upper()
    verb = upper.split(None, 1)[0] if cleaned else ""

    finding = {
        "sql": cleaned,
        "verb": verb,
        "executable": False,
        "count_query": None,
        "table": None,
        "refusals": [],
    }

    if not cleaned:
        finding["refusals"].append("empty statement")
        return finding

    # One statement at a time. A trailing semicolon is fine; an internal one
    # means a second statement is riding along behind the first.
    if ";" in cleaned:
        finding["refusals"].append(
            "more than one statement - only a single statement is allowed")

    if any(upper.startswith(v) for v in FORBIDDEN_VERBS):
        finding["refusals"].append(
            f"{verb} is destructive and is never allowed against a target")
        return finding

    if verb in PROPOSE_ONLY_VERBS:
        finding["refusals"].append(
            f"{verb} changes structure or adds data rather than repairing "
            "bounded damage - this is written out for a human, never run "
            "by the agent")
        return finding

    if verb not in EXECUTABLE_VERBS:
        finding["refusals"].append(
            f"{verb or 'this'} is not a corrective statement the agent knows "
            "how to bound")
        return finding

    match = _DELETE.match(cleaned) or _UPDATE.match(cleaned)
    if not match:
        # This catches the whole unqualified-DELETE family in one test:
        # if it does not parse as VERB ... WHERE ..., it is not bounded.
        finding["refusals"].append(
            "no WHERE clause the agent can identify - an unbounded DELETE or "
            "UPDATE is never allowed")
        return finding

    finding["table"] = match.group("table")
    finding["where"] = match.group("where").strip()

    # The count query is derived from the statement itself, not written
    # separately, so the number reported cannot describe a different set of
    # rows from the one about to change.
    finding["count_query"] = (f"SELECT COUNT(*) FROM {finding['table']} "
                              f"WHERE {finding['where']}")
    finding["executable"] = not finding["refusals"]
    return finding


# ============================================================ CONNECTION

def _connect():
    """The target database, or an explanation of why we cannot reach it.

    Unset TARGET_DB_URL is the normal case and is not an error: the agent
    simply has no way to reach the target, so every repair is proposed
    rather than executed. That is a safe default to ship with.
    """
    database = db.target()
    if not database.configured:
        raise RepairRefused(
            "no target database is configured (TARGET_DB_URL is unset), so "
            "the agent cannot run corrective SQL. The statement and its row "
            "count are posted for a human instead.")
    return database


def count_rows(count_query):
    """How many rows the statement will affect, or None if we cannot tell.

    None is not zero, and the two must never be confused - "I could not
    count" and "there is nothing to delete" lead to opposite actions.
    """
    value = _connect().scalar(count_query)
    return None if value is None else int(value)


# =============================================================== DECIDE

def plan_repair(sql, verdict, incident, max_rows=None):
    """Work out whether this statement may be executed, and say why not.

    Always returns a plan. The plan always contains the SQL, because rule 3
    says the statement is shown whatever happens to it.
    """
    max_rows = config.TARGET_MAX_ROWS if max_rows is None else max_rows
    verdict = verdict or {}

    plan = {
        "incident": incident,
        "cause": verdict.get("likely_cause"),
        "confidence": verdict.get("confidence"),
        "max_rows": max_rows,
        "rows_affected": None,
        "will_execute": False,
        "reason": None,
        **inspect(sql),
    }

    # Rule 1 - there must be a diagnosed cause this statement addresses.
    if verdict.get("fault_zone") != "target":
        plan["reason"] = ("the diagnosis did not place this fault in the "
                          "target, so no target write is justified")
        return plan

    if not plan["cause"] or str(plan["cause"]).lower() in ("unknown", "none", ""):
        plan["reason"] = "no named cause - the agent will not write on a guess"
        return plan

    if plan["refusals"]:
        plan["reason"] = plan["refusals"][0]
        return plan

    if verdict.get("confidence") != "high":
        plan["reason"] = (f"confidence is {verdict.get('confidence')} - a "
                          "target write needs high confidence")
        return plan

    # Rule 2 - count before you change. A count we cannot get is a stop.
    try:
        plan["rows_affected"] = count_rows(plan["count_query"])
    except RepairRefused as error:
        plan["reason"] = str(error)
        return plan
    except Exception as error:
        plan["reason"] = (f"could not count the affected rows "
                          f"({type(error).__name__}: {error}) - and an "
                          "uncounted write is not an informed one")
        return plan

    if plan["rows_affected"] == 0:
        plan["reason"] = ("this statement would affect no rows - the damage "
                          "it describes is not there")
        return plan

    if plan["rows_affected"] > max_rows:
        plan["reason"] = (f"{plan['rows_affected']:,} rows is over the "
                          f"{max_rows:,} row limit for an unattended write")
        return plan

    plan["will_execute"] = True
    plan["reason"] = (f"bounded {plan['verb']} affecting "
                      f"{plan['rows_affected']:,} rows, within the "
                      f"{max_rows:,} row limit, for a named cause")
    return plan


def execute(plan):
    """Run a plan that passed. Logged before and after, always."""
    if not plan.get("will_execute"):
        raise RepairRefused(plan.get("reason") or "this repair was not approved")

    audit({"target_repair": "about_to_run", "incident": plan["incident"],
           "sql": plan["sql"], "rows_expected": plan["rows_affected"],
           "cause": plan["cause"]})

    affected = _connect().execute(plan["sql"])

    audit({"target_repair": "ran", "incident": plan["incident"],
           "sql": plan["sql"], "rows_expected": plan["rows_affected"],
           "rows_actually_affected": affected})

    return {"ran": plan["sql"], "rows_affected": affected,
            "rows_expected": plan["rows_affected"],
            "matched_prediction": (affected == plan["rows_affected"]
                                   if affected is not None else None)}


def describe(plan):
    """The repair, in the words that go into the Slack thread."""
    lines = [f"```{plan['sql']}```"]

    if plan.get("rows_affected") is not None:
        lines.append(f"   affects {plan['rows_affected']:,} rows")
    else:
        lines.append("   row count unknown - not counted, so not run")

    if plan.get("will_execute"):
        lines.append(f"   running it: {plan['reason']}")
    else:
        lines.append(f"   ⛔ not running it: {plan['reason']}")
        lines.append("   → Do this: review the statement above and run "
                     "it yourself if it is right")

    return "\n".join(lines)
