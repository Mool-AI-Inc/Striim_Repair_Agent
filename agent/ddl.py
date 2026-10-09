"""
DDL on the target
=================

The target is a replica. That is the whole argument for letting an agent
change its shape: everything in it came from somewhere else, and anything a
statement removes can be put back if a copy was taken first.

So the rule here is not a permission gate, it is a RECOVERABILITY
requirement. Nothing destructive runs until a named, queryable copy of what
it removes exists in the same database, and the statement that puts it back
has been written out.

THREE CLASSES, ALL OF WHICH EXECUTE.

  additive     nothing is lost, so it runs directly. ADD COLUMN, CREATE
               TABLE, CREATE INDEX, ADD CONSTRAINT that nothing violates,
               and type WIDENING.
  destructive  capture, post, then run. DROP COLUMN, DROP TABLE, DROP
               CONSTRAINT, type NARROWING.
  rename       runs, once nothing else in the pipeline still refers to the
               old name.

AND FOUR THINGS THAT STAY REFUSED, which is the whole list:

  DROP DATABASE, DROP SCHEMA        the blast radius is the wrong shape
  anything with CASCADE             the blast radius is UNKNOWABLE from
                                    here, which is worse
  anything not derived from
  measured evidence                 see below

That last one is the quiet one and it matters most. Every statement this
module runs has to come from `db_describe_table` or a measured schema diff,
and `plan()` refuses without a recorded provenance. A statement inferred
from an error message is a guess with correct syntax - regexes over vendor
prose are how you drop the wrong column.

WHERE UNCERTAINTY GOES. A type change whose direction cannot be determined
is treated as NARROWING, so it gets captured. Being wrong in that direction
costs a redundant backup table; being wrong the other way costs the data.
"""

import re
import time

from . import config
from . import db
from .striim_actions import audit

ADDITIVE = "additive"
DESTRUCTIVE = "destructive"
RENAME = "rename"
REFUSED = "refused"


class DDLRefused(Exception):
    """A statement this module will not run. Always surfaced."""


# ============================================================== SHAPES

_FORMS = (
    ("add_column", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+ADD\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s+(?P<type>.+?)\s*$', re.I | re.S)),
    ("drop_column", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+DROP\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s*$', re.I)),
    ("alter_type", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+ALTER\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s+(?:SET\s+DATA\s+)?TYPE\s+(?P<type>.+?)\s*$',
        re.I | re.S)),
    ("add_constraint", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+ADD\s+CONSTRAINT\s+'
        r'(?P<name>[\w"]+)\s+(?P<body>.+?)\s*$', re.I | re.S)),
    ("drop_not_null", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+ALTER\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s+DROP\s+NOT\s+NULL\s*$', re.I)),
    ("set_not_null", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+ALTER\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s+SET\s+NOT\s+NULL\s*$', re.I)),
    ("drop_constraint", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+DROP\s+CONSTRAINT\s+'
        r'(?P<name>[\w"]+)\s*$', re.I)),
    ("rename_table", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+RENAME\s+TO\s+'
        r'(?P<new>[\w"]+)\s*$', re.I)),
    ("rename_column", re.compile(
        r'^ALTER\s+TABLE\s+(?P<table>[\w."]+)\s+RENAME\s+COLUMN\s+'
        r'(?P<column>[\w"]+)\s+TO\s+(?P<new>[\w"]+)\s*$', re.I)),
    ("create_table", re.compile(
        r'^CREATE\s+TABLE\s+(?P<table>[\w."]+)\s*\((?P<body>.+)\)\s*$',
        re.I | re.S)),
    ("create_index", re.compile(
        r'^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?P<name>[\w"]+)\s+ON\s+'
        r'(?P<table>[\w."]+)\s*\((?P<columns>.+)\)\s*$', re.I | re.S)),
    ("drop_table", re.compile(
        r'^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<table>[\w."]+)\s*$', re.I)),
)

_NEVER = re.compile(r'^\s*DROP\s+(DATABASE|SCHEMA)\b', re.I)
_CASCADE = re.compile(r'\bCASCADE\b', re.I)

# Statements that remove data. These are the ones that ask, because being
# wrong about them costs rows.
_DESTRUCTIVE_FORMS = {"drop_column", "drop_table"}

# Relaxing a constraint removes NO rows and NO values. It widens what the
# table will accept, which is the same direction as widening a column type
# - and widening runs unasked.
#
# This used to sit in _DESTRUCTIVE_FORMS, reasoned as "putting it back
# fails once a null has arrived, so somebody should decide". That reasoning
# is about reversibility, not destruction, and it cost far more than it
# protected: on a pipeline where every write is being rejected, the choice
# is not "relax the constraint or don't", it is "relax the constraint or
# stay down". sarah1_CDC asked four times in one afternoon, was never
# answered, and sat TERMINATED throughout - while the message claimed
# "this one destroys something" about a statement that destroys nothing.
_RELAXING_FORMS = {"drop_constraint", "drop_not_null"}

# Forms that remove no ROWS and no VALUES, so there is nothing to copy
# before running them.
_NO_DATA_LOST = set(_RELAXING_FORMS)


def classify(statement):
    """What kind of change is this, and against what?

    Regexes over a statement THIS AGENT COMPOSED are fine. Regexes over a
    vendor's error message are not, and `plan()` enforces that separately -
    the two are easy to confuse and only one of them is safe.
    """
    text = " ".join(str(statement or "").split()).rstrip(";").strip()
    found = {"statement": text, "kind": REFUSED, "form": None,
             "table": None, "columns": [], "refusals": []}

    if not text:
        found["refusals"].append("empty statement")
        return found
    if ";" in text:
        found["refusals"].append(
            "more than one statement - DDL runs one object at a time, never "
            "in batches")
        return found
    if _CASCADE.search(text):
        found["refusals"].append(
            "CASCADE - what it would take with it cannot be known from here, "
            "so the damage cannot be captured and cannot be undone")
        return found
    if _NEVER.match(text):
        verb = " ".join(text.split()[:2]).upper()
        found["refusals"].append(
            f"{verb} is never run by this agent. A replica's shape is "
            f"repairable; a missing database or schema is not.")
        return found

    for form, pattern in _FORMS:
        match = pattern.match(text)
        if not match:
            continue
        parts = match.groupdict()
        found["form"] = form
        found["table"] = _plain(parts.get("table"))
        if parts.get("column"):
            found["columns"] = [_plain(parts["column"])]
        found.update({k: v for k, v in parts.items()
                      if k in ("new", "name", "type", "body", "columns")})

        if form in _DESTRUCTIVE_FORMS:
            found["kind"] = DESTRUCTIVE
        elif form in _RELAXING_FORMS:
            # Runs unasked, like any other widening. The reversal is still
            # recorded, with its caveat, because putting it back is a real
            # thing somebody may want to do.
            found["kind"] = ADDITIVE
        elif form.startswith("rename"):
            found["kind"] = RENAME
        elif form == "alter_type":
            found["kind"] = ADDITIVE      # revised by plan(), which can look
        else:                             # up the current type
            found["kind"] = ADDITIVE
        return found

    found["refusals"].append(
        "this is not a DDL form the agent recognises, and it will not run a "
        "statement it cannot classify")
    return found


def _plain(name):
    if not name:
        return None
    return ".".join(p.strip('"') for p in str(name).split("."))


# ========================================================= TYPE DIRECTION

_INT_RANK = {"smallint": 1, "int2": 1, "integer": 2, "int": 2, "int4": 2,
             "bigint": 3, "int8": 3}
_TEXTY = {"text", "varchar", "character varying", "char", "character"}


def widens(old, new):
    """Does `new` hold everything `old` could? True / False / None.

    None means "cannot tell", and every caller treats that as narrowing.
    A redundant backup table costs a few megabytes; the other mistake
    costs the column.
    """
    old_name, old_size = _split_type(old)
    new_name, new_size = _split_type(new)

    if old_name == new_name and old_size == new_size:
        return True                       # no change at all

    if old_name in _INT_RANK and new_name in _INT_RANK:
        return _INT_RANK[new_name] >= _INT_RANK[old_name]

    if old_name in _TEXTY and new_name in _TEXTY:
        if new_name == "text":
            return True                   # text holds anything
        if old_name == "text":
            return False                  # text -> varchar(n) truncates
        if old_size is None or new_size is None:
            return None
        return new_size >= old_size

    if old_name == "numeric" and new_name == "numeric":
        if old_size is None or new_size is None:
            return None
        return new_size >= old_size

    return None


def _split_type(declared):
    text = str(declared or "").strip().lower().rstrip(";")
    match = re.match(r"^([a-z ]+?)\s*(?:\(\s*(\d+)\s*[,)])?\s*$", text)
    if not match:
        return text, None
    size = match.group(2)
    return match.group(1).strip(), int(size) if size else None


# =============================================================== PLANNING

def plan(statement, derived_from=None, evidence=None, incident=None,
         key=None, table=None):
    """Turn a statement into something runnable, or explain why not.

    `derived_from` is REQUIRED and is not decoration: it is the record of
    which measurement produced this statement. A DDL statement with no
    provenance is a guess, and this module does not run guesses however
    well-formed they look.
    """
    found = classify(statement)
    found.update({"incident": incident, "derived_from": derived_from,
                  "backup_table": None, "restore": None, "counts": {},
                  "runnable": False, "reason": None})

    if found["refusals"]:
        found["reason"] = found["refusals"][0]
        return found

    if not derived_from:
        found["kind"] = REFUSED
        found["refusals"].append(
            "this statement has no recorded provenance. Every DDL statement "
            "must come from db_describe_table or a measured schema diff - a "
            "statement inferred from an error message is a guess with "
            "correct syntax.")
        found["reason"] = found["refusals"][0]
        return found

    found["table"] = found["table"] or table
    target = db.target()
    if not target.configured:
        found["reason"] = db._no_access("target")
        return found

    # A type change is only additive in one direction, and which direction
    # needs the CURRENT type - which is a measurement, not a guess.
    if found["form"] == "alter_type":
        current = _current_type(found["table"], found["columns"][0])
        direction = widens(current, found.get("type"))
        found["current_type"] = current
        found["widens"] = direction
        if direction is not True:
            found["kind"] = DESTRUCTIVE
            found["counts"]["direction"] = (
                "narrowing" if direction is False else "undetermined")

    # A constraint nothing violates is additive; one that is violated will
    # simply fail, so say so with the number rather than letting the
    # database say it less clearly.
    if found["form"] == "add_constraint":
        violations = _violations(found)
        found["counts"]["violating_rows"] = violations
        if violations:
            found["reason"] = (
                f"{violations:,} row(s) already violate this constraint, so "
                f"adding it would fail. Fix or remove those rows first.")
            return found

    if found["kind"] == RENAME:
        blockers = _still_referenced(found, evidence)
        if blockers:
            found["reason"] = (
                f"the old name is still referenced by {', '.join(blockers)} - "
                f"renaming now would break the pipeline that is reading it")
            return found

    if found["kind"] == DESTRUCTIVE:
        _prepare_capture(found, key)
        if found.get("reason"):
            return found

    # A relaxation takes no backup - there is nothing to copy - but the way
    # to put it back is still written down, caveat and all, because somebody
    # will want it and working it out afterwards is harder than it looks.
    if found["form"] in _RELAXING_FORMS:
        found["restore"] = _restore_statement(found, {})

    found["runnable"] = True
    found["reason"] = found.get("reason") or _summary(found)
    return found


def _summary(found):
    if found["kind"] == ADDITIVE:
        return f"additive - nothing is removed, so it runs directly"
    if found["kind"] == RENAME:
        return "a rename, and nothing else refers to the old name"
    counts = found["counts"]
    return (f"destructive - {counts.get('rows', 0):,} row(s) copied to "
            f"{found['backup_table']} first, and the statement that puts "
            f"them back is recorded")


def _current_type(table, column):
    described = db.db_describe_table("target", table)
    for entry in described.get("columns") or []:
        if entry.get("column_name") == column:
            return entry.get("data_type")
    return None


def _violations(found):
    """How many rows already break the constraint being added."""
    body = str(found.get("body") or "")
    table = db.qualify(found["table"])
    try:
        match = re.search(r"CHECK\s*\((?P<test>.+)\)\s*$", body, re.I | re.S)
        if match:
            return db.target().scalar(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE NOT ({match.group('test')})")
        match = re.search(r"UNIQUE\s*\((?P<cols>[^)]+)\)", body, re.I)
        if match:
            columns = ", ".join(_quote(c.strip())
                                for c in match.group("cols").split(","))
            return db.target().scalar(
                f"SELECT COALESCE(SUM(n) - COUNT(*), 0) FROM "
                f"(SELECT COUNT(*) AS n FROM {table} GROUP BY {columns}) g")
    except Exception:
        return None          # cannot tell; the database will say so
    return 0


def _still_referenced(found, evidence):
    """Does anything in the pipeline still point at the old name?

    A rename that the writer does not know about turns one broken pipeline
    into two, so this reads the component configuration the watcher has
    already gathered rather than assuming.
    """
    old = (found.get("columns") or [None])[0] or found["table"]
    short = str(old).rsplit(".", 1)[-1]
    places = []

    for role in ("source_config", "target_config"):
        props = ((evidence or {}).get(role) or {}).get("properties") or {}
        for name, value in props.items():
            if short and short in str(value):
                places.append(f"{role.replace('_config', '')}.{name}")
    return places


# =============================================================== CAPTURE

def _backup_name(incident):
    """striim_agent_backup_<incident>_<timestamp>, legal and unique.

    Postgres truncates identifiers at 63 characters, and a truncated backup
    name that collides with another is worse than a long one - so the
    timestamp is kept and the incident is what gets shortened.
    """
    stamp = time.strftime("%Y%m%d%H%M%S")
    tag = re.sub(r"[^A-Za-z0-9]+", "_", str(incident or "adhoc")).strip("_")
    prefix = "striim_agent_backup_"
    room = 63 - len(prefix) - len(stamp) - 1
    return f"{prefix}{tag[:max(1, room)]}_{stamp}".lower()


def _prepare_capture(found, key):
    """Work out what to copy, where to, and how to put it back."""
    table = found["table"]
    described = db.db_describe_table("target", table)
    columns = [c["column_name"] for c in described.get("columns") or []]

    if not columns and found["form"] != "drop_table":
        found["reason"] = (f"could not read {table}'s columns, so what this "
                           f"statement removes cannot be captured - and "
                           f"nothing destructive runs uncaptured")
        return

    key = key or (described.get("primary_key") or [None])[0]
    found["key"] = key
    found["backup_table"] = _backup_name(found["incident"])

    if found["form"] == "drop_table":
        found["capture_select"] = "*"
        found["affected"] = columns
    else:
        affected = found["columns"] or []
        if found["form"] in _NO_DATA_LOST:
            affected = []           # a constraint removes no data
        if affected and not key:
            found["reason"] = (
                f"{table} has no primary key, so a copy of {', '.join(affected)} "
                f"could not be matched back to its rows. Nothing destructive "
                f"runs without a way to restore it.")
            return
        wanted = ([key] if key else []) + [c for c in affected if c != key]
        found["capture_select"] = ", ".join(_quote(c) for c in wanted) or "*"
        found["affected"] = affected

    try:
        found["counts"]["rows"] = db.target().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(table)}")
        for column in found.get("affected") or []:
            found["counts"][f"non_null_{column}"] = db.target().scalar(
                f"SELECT COUNT(*) FROM {db.qualify(table)} "
                f"WHERE {_quote(column)} IS NOT NULL")
    except Exception as error:
        found["reason"] = (f"could not count what this would remove "
                           f"({type(error).__name__}: {error}), and an "
                           f"uncounted destructive change is not one anybody "
                           f"can review")
        return

    found["restore"] = _restore_statement(found, described)


def _restore_statement(found, described):
    """The exact statement that puts it back. Written before it is needed."""
    table = db.qualify(found["table"])
    # A relaxation takes no backup, so there is nothing to qualify - and
    # qualifying None raises rather than returning a name.
    backup = db.qualify(found["backup_table"]) if found.get("backup_table") \
        else None
    key = found.get("key")

    if found["form"] == "drop_table":
        return (f"CREATE TABLE {table} AS SELECT * FROM {backup};   "
                f"-- indexes and constraints are NOT restored by this")

    if found["form"] == "drop_column":
        column = found["columns"][0]
        declared = next((c["data_type"] for c in described.get("columns") or []
                         if c["column_name"] == column), "text")
        return (f'ALTER TABLE {table} ADD COLUMN {_quote(column)} {declared}; '
                f'UPDATE {table} t SET {_quote(column)} = b.{_quote(column)} '
                f'FROM {backup} b WHERE t.{_quote(key)} = b.{_quote(key)};')

    if found["form"] == "alter_type":
        column = found["columns"][0]
        return (f'ALTER TABLE {table} ALTER COLUMN {_quote(column)} TYPE '
                f'{found.get("current_type")}; '
                f'UPDATE {table} t SET {_quote(column)} = b.{_quote(column)} '
                f'FROM {backup} b WHERE t.{_quote(key)} = b.{_quote(key)};')

    if found["form"] == "drop_constraint":
        definition = _constraint_definition(found["table"], found.get("name"))
        return (f'ALTER TABLE {table} ADD CONSTRAINT '
                f'{_quote(_plain(found.get("name")))} '
                f'{definition or "<the original definition>"};')

    if found["form"] == "drop_not_null":
        column = found["columns"][0]
        return (f'ALTER TABLE {table} ALTER COLUMN {_quote(column)} '
                f'SET NOT NULL;   -- fails if any row is null by then')

    return None


def _constraint_definition(table, name):
    try:
        rows = db.target().query(
            "SELECT pg_get_constraintdef(c.oid) AS def FROM pg_constraint c "
            "JOIN pg_class t ON t.oid = c.conrelid "
            "WHERE t.relname = %s AND c.conname = %s",
            (str(table).rsplit(".", 1)[-1], _plain(name)))
        return rows[0]["def"] if rows else None
    except Exception:
        return None


def _quote(name):
    return '"' + str(name).strip().strip('"') + '"'


def capture(found):
    """Take the copy. Nothing destructive runs until this has."""
    statement = (f"CREATE TABLE {db.qualify(found['backup_table'])} AS "
                 f"SELECT {found['capture_select']} FROM "
                 f"{db.qualify(found['table'])}")

    audit({"ddl": "capturing", "incident": found.get("incident"),
           "statement": statement, "backup": found["backup_table"],
           "rows_expected": found["counts"].get("rows")})

    db.target().execute(statement)

    held = db.target().scalar(
        f"SELECT COUNT(*) FROM {db.qualify(found['backup_table'])}")
    found["counts"]["backed_up"] = held

    expected = found["counts"].get("rows")
    if expected is not None and held != expected:
        raise DDLRefused(
            f"the backup holds {held:,} rows and the table has "
            f"{expected:,} - refusing to run a destructive statement over an "
            f"incomplete copy")

    audit({"ddl": "captured", "incident": found.get("incident"),
           "backup": found["backup_table"], "rows": held,
           "restore": found.get("restore")})
    return found


def describe(found):
    """What goes into the thread BEFORE anything runs."""
    lines = [f"```{found['statement']};```"]

    counts = found.get("counts") or {}
    if counts.get("rows") is not None:
        lines.append(f"   {counts['rows']:,} rows in {found['table']}")
    for name, value in counts.items():
        if name.startswith("non_null_") and value is not None:
            lines.append(f"   {value:,} of them have a value for "
                         f"{name[len('non_null_'):]}")

    if found.get("backup_table"):
        lines.append(f"   copied to `{found['backup_table']}` first")
    if found.get("restore"):
        lines.append(f"   to undo:\n```{found['restore']}```")
    if found.get("derived_from"):
        lines.append(f"   _derived from {found['derived_from']}_")
    return "\n".join(lines)


# =============================================================== RUNNING

def execute(found, services=None, thread=None):
    """Capture, announce, run, verify. In that order, always."""
    if not found.get("runnable"):
        raise DDLRefused(found.get("reason") or "this statement was not approved")

    if found["kind"] == DESTRUCTIVE:
        capture(found)

    # Announced BEFORE it runs, with the counts and the way back. A
    # destructive change that appears in the thread only after the fact is
    # a notification, not a review.
    if services and thread:
        services.notifier.reply(thread, describe(found), kind="action")

    audit({"ddl": "about_to_run", "incident": found.get("incident"),
           "statement": found["statement"], "kind": found["kind"],
           "counts": found.get("counts"),
           "backup": found.get("backup_table"),
           "restore": found.get("restore"),
           "derived_from": found.get("derived_from")})

    db.target().execute(found["statement"])

    result = {**found, "ran": True, "verified": verify(found)}

    audit({"ddl": "ran", "incident": found.get("incident"),
           "statement": found["statement"], "kind": found["kind"],
           "backup": found.get("backup_table"),
           "restore": found.get("restore"),
           "verified": result["verified"]})
    return result


def verify(found):
    """Re-read the schema and check the change is actually there."""
    if found["form"] == "drop_table":
        exists = db.db_table_exists("target", found["table"]).get("exists")
        return {"ok": exists is False, "detail": f"{found['table']} is gone"
                if exists is False else f"{found['table']} is still present"}

    described = db.db_describe_table("target", found["table"])
    columns = {c["column_name"]: c["data_type"]
               for c in described.get("columns") or []}
    column = (found.get("columns") or [None])[0]

    if found["form"] == "add_column":
        return {"ok": column in columns,
                "detail": f"{column} is present" if column in columns
                          else f"{column} was not added"}
    if found["form"] == "drop_column":
        return {"ok": column not in columns,
                "detail": f"{column} is gone" if column not in columns
                          else f"{column} is still there"}
    if found["form"] == "alter_type":
        now = columns.get(column)
        wanted, _ = _split_type(found.get("type"))
        return {"ok": bool(now) and wanted in str(now).lower(),
                "detail": f"{column} is now {now}"}
    if found["form"] == "rename_column":
        new = _plain(found.get("new"))
        return {"ok": new in columns, "detail": f"{new} is present"
                if new in columns else f"{new} was not created"}

    return {"ok": bool(columns), "detail": f"{found['table']} reads back with "
                                           f"{len(columns)} columns"}


# ======================================================= CREATE FROM SOURCE

def create_table_from_source(source_table, target_table, incident=None):
    """Build a CREATE TABLE from the source's own schema.

    The same source of truth already used to get a restored column's type
    right, applied to the whole table. Not a template and not a guess -
    every column name, type and nullability comes from db_describe_table.
    """
    described = db.db_describe_table("source", source_table)
    columns = described.get("columns") or []
    if not columns:
        return None, (f"the source table {source_table} could not be read, so "
                      f"its column list cannot be reproduced")

    parts = []
    for column in columns:
        piece = f'{_quote(column["column_name"])} {column["data_type"]}'
        if column.get("is_nullable") == "NO":
            piece += " NOT NULL"
        parts.append(piece)

    key = described.get("primary_key") or []
    if key:
        parts.append("PRIMARY KEY (" + ", ".join(_quote(k) for k in key) + ")")

    statement = (f"CREATE TABLE {db.qualify(target_table)} "
                 f"({', '.join(parts)})")
    return statement, (f"the source schema of {source_table}: "
                       f"{len(columns)} columns"
                       + (f", primary key ({', '.join(key)})" if key else ""))
