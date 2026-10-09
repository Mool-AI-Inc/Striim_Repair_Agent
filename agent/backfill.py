"""
Filling a restored column back in
=================================

Adding a dropped column back is only half a repair. The column returns
NULL for every row that existed before it was dropped - those values were
destroyed with the column - and change capture only ever delivers new
changes, so nothing will ever fill them in. The schema is right and the
data is wrong, quietly, forever.

That is a worse state than the visible failure it replaced, because the
pipeline now reports healthy.

TWO DATABASES, NOT ONE. The source and the target may be different servers,
so `UPDATE target SET col = source.col FROM source` is not available and
this must never assume it is. It reads from the source and writes to the
target, in batches, matched on the primary key. On the current deployment
both happen to live on the same Neon host; nothing here depends on that.

THE RULES, all enforced below:

  * `WHERE target.col IS NULL`, on every statement. A value the target
    already holds is never overwritten, and a row that is genuinely NULL at
    source stays NULL - the final count says so rather than hiding it.
  * count first, and report both numbers.
  * batch, with the batch size in the audit log.
  * audit every batch: statement, rows affected, timestamp, incident.
  * re-count afterwards. Never say "backfilled" without a number, and
    never say "done" while rows remain unexplained.

Same discipline as the DDL that precedes it: additive and recoverable, so
it runs - but counted, shown and verified.
"""

import math

from . import config
from . import db
from .striim_actions import audit


class BackfillRefused(Exception):
    """A precondition failed. Always surfaced, never silently skipped."""


def _column(name):
    """Validate a column name and quote it, or refuse to build the SQL."""
    if not db._IDENTIFIER.match(str(name or "").strip()):
        raise ValueError(f"{name!r} is not a plain column name")
    return '"' + str(name).strip().strip('"') + '"'


# ============================================================== PLANNING

COLUMN = "column"
ROWS = "rows"


def plan(source_table, target_table, column=None, incident=None,
         batch_rows=None):
    """What is the target missing, and can it be supplied?

    TWO SCOPES, ONE OPERATION. Which one applies depends on what the
    structural repair put back:

        a restored COLUMN   the rows are there, one value is empty
                            -> UPDATE ... WHERE col IS NULL
        a recreated TABLE   the rows are not there at all
                            -> INSERT ... for keys the target lacks

    Both read from the source and write the difference to the target,
    matched on the primary key, in batches. The second is the first with a
    wider scope, and it is the one that matters more: change capture
    carries changes forward only, so a recreated table stays empty of
    everything that happened before it was recreated.

    Always returns a plan. `runnable` says whether the preconditions hold;
    `reason` says which one did not, in words, because "no primary key, so
    there is no safe way to match rows" belongs in an incident rather than
    in a silent skip.
    """
    batch_rows = batch_rows or config.BACKFILL_BATCH_ROWS
    scope = COLUMN if column else ROWS
    found = {"source_table": source_table, "target_table": target_table,
             "column": column, "scope": scope, "incident": incident,
             "batch_rows": batch_rows, "runnable": False, "key": None,
             "reason": None}

    source_schema = db.db_describe_table("source", source_table)
    target_schema = db.db_describe_table("target", target_table)

    if not source_schema.get("columns") or not target_schema.get("columns"):
        found["reason"] = ("could not read the schema on both sides, so the "
                           "rows cannot be matched safely")
        return found

    key, problem = _shared_key(source_schema, target_schema, scope)
    if problem:
        found["reason"] = problem
        return found
    found["key"] = key

    source_columns = {c["column_name"]: c for c in source_schema["columns"]}
    target_columns = {c["column_name"]: c for c in target_schema["columns"]}
    found["key_type"] = target_columns[key]["data_type"]

    if scope == COLUMN:
        return _plan_column(found, source_columns, target_columns)
    return _plan_rows(found, source_columns, target_columns)


def _shared_key(source_schema, target_schema, scope):
    """One primary key, the same on both sides, or a reason why not."""
    source_key = source_schema.get("primary_key") or []
    target_key = target_schema.get("primary_key") or []

    if not source_key or not target_key:
        missing = "source" if not source_key else "target"
        return None, (f"the {missing} table has no primary key, so there is "
                      f"no safe way to match a source row to the target row "
                      f"it belongs to")

    if source_key != target_key:
        return None, (f"the primary keys differ - {', '.join(source_key)} on "
                      f"the source, {', '.join(target_key)} on the target - "
                      f"so rows cannot be matched")

    if len(target_key) > 1:
        unit = "values" if scope == COLUMN else "rows"
        return None, (f"the primary key is composite "
                      f"({', '.join(target_key)}), which this backfill does "
                      f"not handle. The structure was repaired; the {unit} "
                      f"need filling in by hand.")

    return target_key[0], None


def _plan_column(found, source_columns, target_columns):
    """One column's values, for rows that already exist."""
    column, key = found["column"], found["key"]
    source_table, target_table = found["source_table"], found["target_table"]

    if column not in source_columns:
        found["reason"] = (f"the source has no {column} column either, so "
                           f"there is nothing to copy across")
        return found
    if column not in target_columns:
        found["reason"] = (f"{column} is still not on the target - the DDL "
                           f"did not take effect")
        return found

    found["column_type"] = target_columns[column]["data_type"]

    try:
        found["nulls_in_target"] = db.target().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(target_table)} "
            f"WHERE {_column(column)} IS NULL")
        found["source_has_values"] = db.source().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(source_table)} "
            f"WHERE {_column(column)} IS NOT NULL")
    except Exception as error:
        found["reason"] = (f"could not count the rows involved "
                           f"({type(error).__name__}: {error}), and an "
                           f"uncounted backfill is not an informed one")
        return found

    if not found["nulls_in_target"]:
        found["reason"] = (f"no row in the target has a null {column}, so "
                           f"there is nothing to fill in")
        return found

    if not found["source_has_values"]:
        found["reason"] = (f"the source holds no {column} value for any row, "
                           f"so every null in the target is genuinely null")
        return found

    found["runnable"] = True
    found["reason"] = (
        f"{found['nulls_in_target']:,} row(s) in the target have no {column}; "
        f"the source holds a {column} for {found['source_has_values']:,} "
        f"row(s). Matching on {key}, {found['batch_rows']:,} rows per "
        f"statement.")
    return found


def _plan_rows(found, source_columns, target_columns):
    """Whole rows, for keys the target does not have at all."""
    key = found["key"]
    source_table, target_table = found["source_table"], found["target_table"]

    # Only columns BOTH sides have. A target column the source does not
    # send stays at its default; a source column the target lacks is a
    # schema problem, and it is not this function's job to hide it.
    shared = [c["column_name"] for c in
              sorted(target_columns.values(),
                     key=lambda c: list(target_columns).index(c["column_name"]))
              if c["column_name"] in source_columns]
    if key not in shared:
        found["reason"] = (f"{key} is not readable on both sides, so rows "
                           f"cannot be matched")
        return found
    found["columns"] = shared

    try:
        found["source_rows"] = db.source().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(source_table)}")
        found["target_rows"] = db.target().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(target_table)}")
    except Exception as error:
        found["reason"] = (f"could not count the rows involved "
                           f"({type(error).__name__}: {error}), and an "
                           f"uncounted backfill is not an informed one")
        return found

    # MEASURED, not inferred from the difference between two totals - the
    # target can hold rows the source no longer has, and then the totals
    # would disagree while nothing at all was missing.
    try:
        found["keys_missing"] = _count_missing_keys(found)
    except Exception as error:
        found["reason"] = (f"could not work out which keys the target is "
                           f"missing ({type(error).__name__}: {error})")
        return found

    if not found["source_rows"]:
        found["reason"] = ("the source table is empty, so there is nothing "
                           "to copy across")
        return found

    if not found["keys_missing"]:
        found["reason"] = (f"the target already holds every key the source "
                           f"has ({found['target_rows']:,} rows), so there "
                           f"is nothing to fill in")
        return found

    found["runnable"] = True
    found["reason"] = (
        f"the target is missing {found['keys_missing']:,} row(s); the source "
        f"holds {found['source_rows']:,}. Matching on {key}, "
        f"{found['batch_rows']:,} rows per statement.")
    return found


def _count_missing_keys(found):
    """How many source keys the target does not have. Exact.

    Short-circuits the common case - a table that has just been recreated
    holds nothing, so every source row is missing and one COUNT settles it.
    Otherwise it walks the source keys and asks the target which it
    already has, which is two reads and no join across the two servers.
    """
    if not found.get("target_rows"):
        return found["source_rows"]

    key = _column(found["key"])
    source, target = db.source(), db.target()
    table_s, table_t = (db.qualify(found["source_table"]),
                        db.qualify(found["target_table"]))
    batch = int(found["batch_rows"])

    missing, last = 0, None
    while True:
        where = f" WHERE {key} > %s" if last is not None else ""
        keys = source.query(
            f"SELECT {key} AS k FROM {table_s}{where} "
            f"ORDER BY {key} LIMIT {batch}",
            (last,) if last is not None else ())
        if not keys:
            return missing

        wanted = [row["k"] for row in keys]
        last = wanted[-1]
        present = target.query(
            f"SELECT {key} AS k FROM {table_t} WHERE {key} = ANY(%s)",
            (wanted,))
        missing += len(wanted) - len(present)


def describe(found):
    """The plan, in the words that go into the incident thread."""
    if not found.get("runnable"):
        return f"_no backfill: {found.get('reason')}_"

    if found.get("scope") == ROWS:
        return (f"*backfill {found['target_table']}* — {found['reason']}\n"
                f"    Only keys the target does not have are inserted; every "
                f"row already there is left exactly as it is.")

    return (f"*backfill {found['column']}* — {found['reason']}\n"
            f"    Only rows where {found['column']} IS NULL are touched; "
            f"nothing already in the target is overwritten.")


# ============================================================= RUNNING

def run(found):
    """Copy the difference across, in batches, and count what happened."""
    if not found.get("runnable"):
        raise BackfillRefused(found.get("reason") or "the backfill was not planned")

    if found.get("scope") == ROWS:
        return _run_rows(found)
    return _run_column(found)


def _run_rows(found):
    """Insert the rows the target does not have.

    Keyset pagination over the SOURCE, because that is the side that has
    the rows. Two guarantees against overwriting anything: only keys the
    target was asked about and did not have are inserted, and every
    statement carries ON CONFLICT DO NOTHING - so even a row that arrived
    between the check and the insert is left exactly as it is.
    """
    source, target = db.source(), db.target()
    table_s, table_t = (db.qualify(found["source_table"]),
                        db.qualify(found["target_table"]))
    key = _column(found["key"])
    columns = found["columns"]
    quoted = ", ".join(_column(c) for c in columns)
    batch_rows = int(found["batch_rows"])

    audit({"backfill": "starting", "scope": ROWS,
           "incident": found.get("incident"),
           "target": found["target_table"], "key": found["key"],
           "columns": columns, "batch_rows": batch_rows,
           "keys_missing": found["keys_missing"],
           "source_rows": found["source_rows"]})

    inserted, batches, last = 0, 0, None
    max_batches = math.ceil(found["source_rows"] / batch_rows) + 5

    while batches < max_batches:
        where = f" WHERE {key} > %s" if last is not None else ""
        rows = source.query(
            f"SELECT {quoted} FROM {table_s}{where} "
            f"ORDER BY {key} LIMIT {batch_rows}",
            (last,) if last is not None else ())
        if not rows:
            break

        last = rows[-1][found["key"]]
        batches += 1

        # Ask the target which of these it already has, and send only the
        # remainder. A row the target holds is never touched.
        wanted = [row[found["key"]] for row in rows]
        present = {r["k"] for r in target.query(
            f"SELECT {key} AS k FROM {table_t} WHERE {key} = ANY(%s)",
            (wanted,))}
        fresh = [row for row in rows if row[found["key"]] not in present]
        if not fresh:
            continue

        placeholders = ", ".join(
            ["(" + ", ".join(["%s"] * len(columns)) + ")"] * len(fresh))
        statement = (f"INSERT INTO {table_t} ({quoted}) VALUES "
                     f"{placeholders} ON CONFLICT DO NOTHING")
        values = [row[c] for row in fresh for c in columns]

        affected = target.execute(statement, values)
        inserted += affected or 0

        audit({"backfill": "batch", "scope": ROWS,
               "incident": found.get("incident"), "batch": batches,
               "rows_offered": len(fresh), "rows_affected": affected,
               "statement": statement[:300] +
                            (" …" if len(statement) > 300 else "")})

    result = {**found, "batches": batches, "rows_filled": inserted,
              "hit_the_loop_guard": batches >= max_batches}
    return verify(result)


def _run_column(found):
    """Fill in one column's values, for rows that already exist.

    Keyset pagination rather than OFFSET: the set of NULL rows shrinks as
    we go, so an offset would skip rows. Walking forward by primary key is
    stable under our own updates.
    """
    target, source = db.target(), db.source()
    table_t, table_s = db.qualify(found["target_table"]), db.qualify(found["source_table"])
    key, column = _column(found["key"]), _column(found["column"])
    key_type, column_type = found["key_type"], found["column_type"]
    batch_rows = found["batch_rows"]

    audit({"backfill": "starting", "scope": COLUMN,
           "incident": found.get("incident"),
           "target": found["target_table"], "column": found["column"],
           "key": found["key"], "batch_rows": batch_rows,
           "nulls_before": found["nulls_in_target"]})

    filled, batches, last_key = 0, 0, None
    # A loop guard, not a permission gate: bounded by the work there is to
    # do plus slack, so a bug cannot spin against a customer's database.
    max_batches = math.ceil(found["nulls_in_target"] / batch_rows) + 5

    while batches < max_batches:
        where = f"{column} IS NULL" + (f" AND {key} > %s" if last_key is not None else "")
        keys = target.query(
            f"SELECT {key} AS k FROM {table_t} WHERE {where} "
            f"ORDER BY {key} LIMIT {int(batch_rows)}",
            (last_key,) if last_key is not None else ())
        if not keys:
            break

        wanted = [row["k"] for row in keys]
        last_key = wanted[-1]

        # Read from the SOURCE - a separate connection, possibly a separate
        # server. No join across the two is available and none is assumed.
        supplied = source.query(
            f"SELECT {key} AS k, {column} AS v FROM {table_s} "
            f"WHERE {key} = ANY(%s) AND {column} IS NOT NULL",
            (wanted,))

        batches += 1
        if not supplied:
            continue

        pairs = [(row["k"], row["v"]) for row in supplied]
        placeholders = ",".join(["(%s,%s)"] * len(pairs))
        statement = (
            f"UPDATE {table_t} AS t SET {column} = v.val::{column_type} "
            f"FROM (VALUES {placeholders}) AS v(k, val) "
            f"WHERE t.{key} = v.k::{key_type} AND t.{column} IS NULL")

        affected = target.execute(statement, [x for pair in pairs for x in pair])
        filled += affected or 0

        audit({"backfill": "batch", "incident": found.get("incident"),
               "batch": batches, "rows_offered": len(pairs),
               "rows_affected": affected,
               "statement": statement[:300] + (" …" if len(statement) > 300 else "")})

    result = {**found, "batches": batches, "rows_filled": filled,
              "hit_the_loop_guard": batches >= max_batches}
    return verify(result)


# ============================================================== VERIFY

def verify(result):
    """Re-count. Never report "backfilled" without a number.

    A remaining gap is not a failure on its own - a value that is NULL at
    source stays NULL, and a row deleted at source since is meant to be
    absent. What matters is that the number is stated rather than rounded
    away, and that the likely reason is named.
    """
    if result.get("scope") == ROWS:
        return _verify_rows(result)
    return _verify_column(result)


def _verify_rows(result):
    """Both totals, and the keys still missing - measured, not inferred."""
    try:
        result["source_rows_after"] = db.source().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(result['source_table'])}")
        result["target_rows_after"] = db.target().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(result['target_table'])}")
        result["keys_missing_after"] = _count_missing_keys(
            {**result, "target_rows": result["target_rows_after"],
             "source_rows": result["source_rows_after"]})
    except Exception as error:
        result["keys_missing_after"] = None
        result["note"] = (f"backfilled {result['rows_filled']:,} row(s), but "
                          f"the result could not be re-counted "
                          f"({type(error).__name__}) - treat this as "
                          f"unverified")
        audit({"backfill": "finished", "scope": ROWS,
               "incident": result.get("incident"),
               "rows_filled": result["rows_filled"], "verified": False})
        return result

    remaining = result["keys_missing_after"]
    if remaining == 0:
        result["note"] = (
            f"backfilled {result['rows_filled']:,} row(s); source and target "
            f"both hold every key the source has "
            f"({result['source_rows_after']:,} rows on the source, "
            f"{result['target_rows_after']:,} on the target)")
    else:
        result["note"] = (
            f"backfilled {result['rows_filled']:,} row(s); the target is "
            f"still missing {remaining:,} key(s). Most likely those rows "
            f"were deleted at source after this started - they cannot be "
            f"copied from something that no longer holds them.")

    if result.get("hit_the_loop_guard"):
        result["note"] += (" NOTE: the batch limit was reached, so this may "
                           "be incomplete - run it again.")

    audit({"backfill": "finished", "scope": ROWS,
           "incident": result.get("incident"),
           "rows_filled": result["rows_filled"],
           "keys_missing_after": remaining,
           "source_rows": result["source_rows_after"],
           "target_rows": result["target_rows_after"],
           "batches": result["batches"]})
    return result


def _verify_column(result):
    try:
        remaining = db.target().scalar(
            f"SELECT COUNT(*) FROM {db.qualify(result['target_table'])} "
            f"WHERE {_column(result['column'])} IS NULL")
    except Exception as error:
        result["nulls_after"] = None
        result["note"] = (f"backfilled {result['rows_filled']:,} row(s), but "
                          f"the remaining nulls could not be counted "
                          f"({type(error).__name__}) - treat this as "
                          f"unverified")
        audit({"backfill": "finished", "incident": result.get("incident"),
               "rows_filled": result["rows_filled"], "nulls_after": None})
        return result

    result["nulls_after"] = remaining
    column = result["column"]

    if remaining == 0:
        result["note"] = (f"backfilled {result['rows_filled']:,} row(s); no "
                          f"row is left with a null {column}")
    else:
        result["note"] = (
            f"backfilled {result['rows_filled']:,} row(s); {remaining:,} "
            f"row(s) still have a null {column}. The source holds no value "
            f"for those, so they are genuinely null rather than missed - "
            f"unless the source has changed since this ran.")

    if result.get("hit_the_loop_guard"):
        result["note"] += (" NOTE: the batch limit was reached, so this may "
                           "be incomplete - run it again.")

    audit({"backfill": "finished", "scope": COLUMN,
           "incident": result.get("incident"),
           "rows_filled": result["rows_filled"], "nulls_after": remaining,
           "batches": result["batches"]})
    return result


# ================================================================ TOOL

def preview(source_table, target_table, column=None):
    """Read-only: what a backfill would do. Nothing is written.

    Omit `column` to ask about whole missing ROWS rather than one column's
    values - which is the question after a table has been recreated.
    """
    found = plan(source_table, target_table, column)
    common = {
        "source_table": source_table, "target_table": target_table,
        "scope": found["scope"], "column": column,
        "would_run": found["runnable"],
        "matched_on": found.get("key"),
        "batch_rows": found.get("batch_rows"),
        "note": found.get("reason"),
    }

    if found["scope"] == ROWS:
        return {**common,
                "rows_missing_from_target": found.get("keys_missing"),
                "rows_in_source": found.get("source_rows"),
                "rows_in_target": found.get("target_rows")}

    return {**common,
            "rows_with_null_in_target": found.get("nulls_in_target"),
            "rows_with_a_value_in_source": found.get("source_has_values")}
