"""
Schema drift — the fault that has not happened yet
==================================================

With schema evolution set to Ignore, a DDL statement on the source is
captured and then deliberately not forwarded. Nothing breaks. Striim is
doing exactly what it was told. The target simply falls behind, silently,
and stays that way until some write happens to need a column that was
never added - which can be hours later, and by then it looks like an
unrelated fault.

Two things make it visible before that:

    the ignored-DDL counter    ticks the MOMENT somebody alters the source
    a schema comparison        shows the target missing what the source
                               gained, whenever we care to look

So this runs on a SCHEDULE, for every monitored pipeline, rather than only
when something has already broken. By the time a fault brings the agent
here on its own, the warning window has been and gone.

A WARNING IS NOT AN INCIDENT, and the distinction is the whole point.

    incident   something is broken, here is what I did about it
    warning    nothing is broken, and something will break

An incident interrupts. A warning informs. Giving drift the incident
treatment - a thread, a red circle, a tier, a repair - would say
"production is down" about a pipeline that is running perfectly well, and
the third time that happens nobody reads the channel any more.

Nothing here repairs anything. It reports, once per distinct drift, and
stops.
"""

import time

from . import config
from . import db
from . import striim_tools as st


def check(striim, app, source_table=None, target_table=None):
    """Has the target fallen behind the source? Never raises.

    Returns a report whether or not anything is wrong; `drifted` says
    which. Everything it could not determine says so rather than reading
    as a clean result - a comparison that did not happen is not a
    comparison that matched.
    """
    report = {"app": app, "at": time.time(), "drifted": False,
              "checked": False, "findings": [], "note": None}

    flow = st.get_flow_stats(striim, app)
    source = (flow.get("source") or {}).get("component")
    target = (flow.get("target") or {}).get("component")

    # The ignored-DDL counter first: it is the earliest signal there is,
    # and it needs no database access at all.
    report["ddl"] = st.ddl_metrics(striim, app)
    ignored = report["ddl"].get("ignored_ddl_count")
    if ignored:
        report["checked"] = True
        report["drifted"] = True
        report["findings"].append({
            "kind": "ignored_ddl",
            "detail": (f"{ignored} schema change(s) from the source have "
                       f"been captured and deliberately not applied to the "
                       f"target"),
            "when": report["ddl"].get("last_ddl_at"),
            "statement": report["ddl"].get("last_ddl_statement"),
        })

    if not (source and target):
        report["note"] = ("could not identify both ends of this pipeline, so "
                          "the schemas were not compared")
        return report

    described = {"source": st.describe_component(striim, source),
                 "target": st.describe_component(striim, target)}
    for role, entry in described.items():
        db.remember(role, entry)

    source_table = source_table or _table_of(described["source"], first=True)
    target_table = target_table or _table_of(described["target"], first=False)

    if not (source_table and target_table):
        report["note"] = ("the source and target tables are not both named in "
                          "the pipeline configuration, so the schemas were "
                          "not compared")
        return report

    report["source_table"], report["target_table"] = source_table, target_table

    columns = db.db_compare_schema(source_table, target_table)
    report["schema"] = columns
    if columns.get("checked"):
        report["checked"] = True
        for name in columns.get("missing_in_target") or []:
            report["drifted"] = True
            report["findings"].append({
                "kind": "missing_column", "column": name,
                "type": (columns.get("source_columns") or {}).get(name),
                "detail": (f"the source has {name} "
                           f"({(columns.get('source_columns') or {}).get(name)}) "
                           f"and the target does not"),
            })
        for difference in columns.get("differing_types") or []:
            if difference.get("widening"):
                report["drifted"] = True
                report["findings"].append({
                    "kind": "narrower_column", "column": difference["column"],
                    "detail": (f"{difference['column']} is "
                               f"{difference['source_type']} on the source "
                               f"and {difference['target_type']} on the "
                               f"target"),
                })

    constraints = db.db_compare_constraints(source_table, target_table)
    report["constraints"] = constraints
    for name in constraints.get("rejecting") or []:
        report["checked"] = True
        report["drifted"] = True
        entry = next((c for c in constraints["only_on_target"]
                      if c["name"] == name), {})
        report["findings"].append({
            "kind": "extra_constraint", "constraint": name,
            "detail": (f"the target enforces {name} "
                       f"({entry.get('definition')}) and the source does not"),
        })

    if not report["checked"]:
        report["note"] = ("nothing could be compared - treat this as 'not "
                          "checked', not as 'no drift'")
    elif not report["drifted"]:
        report["note"] = "the target matches the source"
    return report


def _table_of(described, first):
    props = {str(k).lower(): v for k, v in
             (described.get("properties") or {}).items()}
    raw = str(props.get("tables") or "").strip()
    if not raw or "%" in raw:
        return None
    names = [t.strip() for t in raw.split(",") if t.strip()]
    if not names:
        return None
    return names[0] if first else names[-1]


def fingerprint(report):
    """One key per distinct drift, so it is said once and not every poll."""
    parts = sorted(
        f"{f['kind']}:{f.get('column') or f.get('constraint') or ''}"
        for f in report.get("findings") or [])
    return f"{report['app']}|" + ",".join(parts)


def compose(report):
    """The warning, worded as a warning.

    No red circle, no thread, no tier - and it says plainly that nothing
    is broken, because the reader's first question on seeing anything from
    this agent is "is production down".
    """
    name = report["app"].rsplit(".", 1)[-1]
    lines = [f"🟡  *{name}* — heads up, nothing is broken yet", ""]

    for finding in report["findings"]:
        when = _ago(finding.get("when"))
        if finding["kind"] == "missing_column":
            lines.append(
                f"    The source gained *{finding['column']}* "
                f"({finding.get('type')}){when}. The target does not have "
                f"it, so writes carrying that column will fail.")
        elif finding["kind"] == "narrower_column":
            lines.append(f"    {finding['detail']} — values too long for the "
                         f"target will be refused.")
        elif finding["kind"] == "extra_constraint":
            lines.append(f"    {finding['detail']} — rows the source thinks "
                         f"are fine will be refused and dropped.")
        elif finding["kind"] == "ignored_ddl":
            lines.append(f"    {finding['detail']}{when}.")
            if finding.get("statement"):
                lines.append(f"        {finding['statement']}")

    lines += ["", "    Nothing has failed yet — the pipeline is running and "
                  "the rows going through it do not need this. I will repair "
                  "it automatically when something does."]
    return "\n".join(lines)


def _ago(when):
    if not when:
        return ""
    try:
        seconds = time.time() - float(when)
    except (TypeError, ValueError):
        return f" ({when})"
    if seconds < 90:
        return " just now"
    if seconds < 5400:
        return f" {round(seconds / 60)} minutes ago"
    return f" {round(seconds / 3600)} hours ago"
