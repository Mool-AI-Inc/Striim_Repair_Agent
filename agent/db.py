"""
Direct database access
======================

Until now the agent could see that a pipeline had stopped and could not
find out whether the thing it depends on had come back. Every HALT ended in
the same refusal - "I cannot tell whether the cause has cleared" - because
Striim's control plane only reports Striim's opinion of the world. This is
the other end of the pipe.

TWO CONNECTIONS, AND THEY ARE NOT THE SAME KIND OF THING.

    TARGET   full execution rights except DDL. This is an auto-repair
             agent: it fixes what it can reach.
    SOURCE   READ ONLY, always, with no exceptions and no override.

The source is somebody else's system of record. A wrong write there is
unrecoverable and invisible to us. So it is not protected by a policy that
could be argued with - it is protected structurally, in three independent
layers:

  1. `SourceDB` has no `execute()`. There is no method to call.
  2. Every statement is checked against a SELECT/WITH allowlist before it
     is sent.
  3. The connection itself is opened read-only, so the server refuses a
     write even if the first two were somehow bypassed.

Any one of those would do. Having three means a future refactor has to
defeat all of them deliberately rather than one of them by accident.

CREDENTIALS COME FROM THE ENVIRONMENT, never from Striim's stored
configuration. We do not harvest secrets out of the platform, even though
`describe_component` puts a connection URL within easy reach.

    TARGET_DB_URL=postgresql://user:pass@host:5432/db?sslmode=require
    SOURCE_DB_URL=postgresql://...        optional, READ ONLY

With neither set every function here says "not configured" in words and the
agent carries on without them - less capable, never guessing.
"""

import os
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlparse

from . import config

# The single point where this package touches a database driver. Tests
# replace it wholesale, which is how the whole module above this line runs
# against a real SQL engine with no Postgres anywhere. Same idea as
# Striim.offline(): one seam, real code above it.
CONNECT = None


def _raw_connect(conninfo, readonly=False):
    """Open a connection. The only psycopg2 call in the package.

    `conninfo` is a dict of psycopg2 keyword arguments, or {"dsn": url}
    when an explicit URL override is in play. Keywords are preferred
    because a discovered password is an arbitrary string and putting it
    through URL encoding is one more place to get it wrong.
    """
    if CONNECT is not None:
        return CONNECT(conninfo, readonly=readonly)

    import psycopg2

    options = [f"-c statement_timeout={config.DB_STATEMENT_TIMEOUT_MS}"]
    if readonly:
        # Layer 3. The server itself refuses writes on this connection.
        options.append("-c default_transaction_read_only=on")

    kwargs = dict(conninfo)
    dsn = kwargs.pop("dsn", None)
    kwargs.update(connect_timeout=config.DB_CONNECT_TIMEOUT,
                  options=" ".join(options))

    connection = (psycopg2.connect(dsn, **kwargs) if dsn
                  else psycopg2.connect(**kwargs))
    if readonly:
        connection.set_session(readonly=True, autocommit=True)
    return connection


# ======================================================== DISCOVERY
#
# Striim already knows where the databases are. `describe_component` on a
# DatabaseWriter returns:
#
#     "connectionurl": "jdbc:postgresql://ep-xxx.neon.tech:5432/neondb?sslmode=require"
#     "username":      "neondb_owner"
#     "password":      "com.webaction.security.PasswordPlatform@173fb898"
#     "password_encrypted": true
#
# Host, port, database and user are in plaintext. The password is a Java
# object handle - not a password, not extractable, and it should not be.
# That is the right design on Striim's part and we do not try to defeat it:
# the agent discovers everything it can and asks the operator for the one
# thing it cannot have, naming the exact variable and the exact host.
#
# So a pipeline usually needs NO configuration beyond one password, instead
# of a hand-written connection URL per database.

DEFAULT_PORT = 5432

# Non-Postgres endpoints are recognised and declined rather than guessed at.
_JDBC_DIALECT = re.compile(r"^jdbc:([a-z0-9]+):", re.I)


@dataclass
class Endpoint:
    """Where a database is, and whether we can get into it."""

    role: str
    host: str = ""
    port: int = DEFAULT_PORT
    database: str = ""
    user: str = ""
    options: dict = field(default_factory=dict)
    component: str = ""
    dialect: str = "postgresql"
    url_override: str = ""
    password: str = ""
    password_from: str = ""          # which variable supplied it
    problem: str = ""                # why it is unusable, in words

    @property
    def discovered(self):
        """Do we know WHERE it is, whether or not we can get in?"""
        return bool(self.url_override or (self.host and self.database))

    @property
    def usable(self):
        return bool(self.url_override or
                    (self.host and self.database and self.user
                     and self.password and not self.problem))

    def connect_kwargs(self):
        if self.url_override:
            return {"dsn": self.url_override}
        kwargs = {"host": self.host, "port": self.port,
                  "dbname": self.database, "user": self.user,
                  "password": self.password}
        if self.options.get("sslmode"):
            kwargs["sslmode"] = self.options["sslmode"]
        return kwargs

    def where(self):
        if self.url_override:
            return "the database configured in TARGET_DB_URL"
        return f"{self.host}:{self.port}/{self.database}"

    def redacted(self):
        """Everything about this endpoint except the secret."""
        return {
            "role": self.role,
            "discovered_from": self.component or "TARGET_DB_URL",
            "host": self.host or None,
            "port": self.port if self.host else None,
            "database": self.database or None,
            "user": self.user or None,
            "sslmode": self.options.get("sslmode"),
            "dialect": self.dialect,
            "password": (f"set, from {self.password_from}" if self.password
                         else "NOT SET"),
            "usable": self.usable,
            "note": self.problem or (
                f"ready — {self.where()} as {self.user}"
                if self.usable else "not usable yet"),
        }


def parse_jdbc(url):
    """host / port / database / options out of a JDBC connection string.

    Returns None for anything that is not a Postgres URL - being honest
    about an Oracle endpoint beats producing a plausible-looking Postgres
    connection that could never work.
    """
    raw = str(url or "").strip()
    if not raw:
        return None

    dialect = None
    match = _JDBC_DIALECT.match(raw)
    if match:
        dialect = match.group(1).lower()
        raw = raw[len("jdbc:"):]

    if dialect and dialect not in ("postgresql", "postgres"):
        return {"dialect": dialect, "unsupported": True}

    if "://" not in raw:
        return None

    parsed = urlparse(raw)
    if not parsed.hostname:
        return None

    return {
        "dialect": "postgresql",
        "host": parsed.hostname,
        "port": parsed.port or DEFAULT_PORT,
        "database": (parsed.path or "").lstrip("/") or "",
        "options": dict(parse_qsl(parsed.query or "")),
    }


def password_variables(host):
    """Every variable name accepted for this host, best-known first.

    The exact host is accepted because it is the obvious thing to type; the
    sanitised form is accepted because dots and hyphens are awkward to set
    as environment variables on most shells, and it is the one the agent
    recommends.
    """
    safe = "STRIIM_DBPASS_" + re.sub(r"[^A-Za-z0-9]+", "_", host or "").upper().strip("_")
    return [f"STRIIM_DBPASS_{host}", safe, "STRIIM_DBPASS_DEFAULT"]


def find_password(host):
    """(password, which variable). Read at call time, never at import.

    Deliberately not in config.py: these names are keyed by whatever host a
    pipeline turns out to point at, so they cannot be enumerated when the
    process starts.
    """
    for name in password_variables(host):
        value = os.environ.get(name)
        if value:
            return value, name
    return "", ""


def discover(described, role):
    """Build an Endpoint from a described component. Never raises."""
    endpoint = Endpoint(role=role)

    override = config.TARGET_DB_URL if role == "target" else config.SOURCE_DB_URL
    if override:
        endpoint.url_override = override
        endpoint.component = f"{role.upper()}_DB_URL"
        return endpoint

    if not described or described.get("error"):
        endpoint.problem = (
            f"the {role} component's configuration could not be read, so "
            f"there is nothing to discover a connection from")
        return endpoint

    endpoint.component = described.get("component") or ""
    props = {str(k).lower(): v for k, v in
             (described.get("properties") or {}).items()}

    parsed = parse_jdbc(props.get("connectionurl") or props.get("connectionurl "))
    if parsed is None:
        endpoint.problem = (
            f"{endpoint.component or 'this component'} records no database "
            f"connection URL, so it is not a database endpoint this agent "
            f"can reach")
        return endpoint

    if parsed.get("unsupported"):
        endpoint.dialect = parsed["dialect"]
        endpoint.problem = (
            f"{endpoint.component} connects to {parsed['dialect']}, and this "
            f"agent only speaks Postgres. It will diagnose this pipeline "
            f"from Striim's control plane and will not query the database.")
        return endpoint

    endpoint.host = parsed["host"]
    endpoint.port = parsed["port"]
    endpoint.database = parsed["database"]
    endpoint.options = parsed["options"]
    endpoint.user = str(props.get("username") or props.get("user") or "")

    endpoint.password, endpoint.password_from = find_password(endpoint.host)

    if not endpoint.user:
        endpoint.problem = (
            f"{endpoint.where()} was discovered from {endpoint.component}, "
            f"but it records no username, so the agent cannot connect.")
    elif not endpoint.password:
        # Item 4: never "no database access configured". Say exactly which
        # variable, for exactly which host.
        _, recommended, fallback = password_variables(endpoint.host)
        endpoint.problem = (
            f"the {role} database is {endpoint.where()} as "
            f"{endpoint.user}, discovered from {endpoint.component}. Striim "
            f"stores the password as an encrypted object that cannot be "
            f"read back, so set it in the environment:\n"
            f"    {recommended}=<the password for {endpoint.user}>\n"
            f"or {fallback} if one password covers every host.")

    return endpoint


# What discovery has found so far, per role. Populated by the EVIDENCE node,
# which is where component configurations are read anyway - so the database
# tools do not each need a Striim client of their own.
_DISCOVERED = {}


def remember(role, described):
    """Record what a described component says about its database."""
    endpoint = discover(described, role)
    _DISCOVERED[role] = endpoint
    return endpoint


def forget():
    _DISCOVERED.clear()


def endpoint_for(role):
    """The endpoint for a role: override, then discovery, then nothing."""
    override = config.TARGET_DB_URL if role == "target" else config.SOURCE_DB_URL
    if override:
        return Endpoint(role=role, url_override=override,
                        component=f"{role.upper()}_DB_URL")
    return _DISCOVERED.get(role)


# Statements a read-only connection may carry. Anything else never reaches
# the wire, whatever the connection flags say.
_READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH)\b", re.I)

# A table name we are willing to interpolate. Optionally schema-qualified,
# optionally double-quoted, nothing else - no spaces, no semicolons, no
# comment markers. Table names reach here from a model and from config, so
# this is the boundary where an identifier stops being a string.
_IDENTIFIER = re.compile(r'^"?[A-Za-z_][A-Za-z0-9_$]*"?'
                         r'(\."?[A-Za-z_][A-Za-z0-9_$]*"?)?$')


class NotConfigured(Exception):
    """No URL for this database. Normal, and never a guess."""


class WriteRefused(Exception):
    """Something tried to write through a read-only connection."""


def qualify(table):
    """Validate a table name and return it quoted, or raise.

    Rejecting is the point. Everything downstream interpolates this into
    SQL, so an identifier that does not look like an identifier must stop
    here rather than three layers further in.
    """
    if not table or not _IDENTIFIER.match(str(table).strip()):
        raise ValueError(
            f"{table!r} is not a plain table name. Names reach this agent "
            f"from a diagnosis, so anything that is not a bare identifier - "
            f"optionally schema-qualified - is refused rather than "
            f"interpolated into SQL.")

    parts = str(table).strip().split(".")
    return ".".join(f'"{p.strip(chr(34))}"' for p in parts)


# ============================================================ CONNECTIONS

class _Database:
    """Shared read behaviour. Neither subclass is instantiated directly."""

    which = "database"
    readonly = True

    def __init__(self, endpoint=None):
        # A bare string is still accepted, so an explicit URL remains a
        # one-liner for anyone who wants to bypass discovery entirely.
        if isinstance(endpoint, str):
            endpoint = Endpoint(role=self.which, url_override=endpoint,
                                component=f"{self.which.upper()}_DB_URL")
        self.endpoint = endpoint

    @property
    def configured(self):
        return bool(self.endpoint and self.endpoint.usable)

    def _connect(self):
        if not self.configured:
            raise NotConfigured(_no_access(self.which))
        return _raw_connect(self.endpoint.connect_kwargs(),
                            readonly=self.readonly)

    def query(self, sql, params=None):
        """Run a SELECT and return a list of dicts."""
        if not _READ_ONLY_SQL.match(sql):
            raise WriteRefused(
                f"only SELECT and WITH may be sent through query(); got "
                f"{sql.strip().split(None, 1)[0].upper()!r}")

        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params or ())
            if cursor.description is None:
                return []
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            connection.close()

    def scalar(self, sql, params=None):
        rows = self.query(sql, params)
        if not rows:
            return None
        return next(iter(rows[0].values()))


class SourceDB(_Database):
    """READ ONLY. There is deliberately no execute() on this class."""

    which = "source"
    readonly = True


class TargetDB(_Database):
    """Full rights except DDL, which is refused in target_repair."""

    which = "target"
    readonly = False

    def execute(self, sql, params=None):
        """Run one statement and return the number of rows it changed.

        Committed on success, rolled back on any error. `rowcount` of -1 is
        psycopg2 for "I do not know", and is passed through as None rather
        than being flattened to zero - the difference matters when the
        incident reports how many rows moved.

        `params` are bound by the driver, never interpolated. A backfill
        carries customer data back into a customer's database, and that
        data is the last thing that should be pasted into a string.
        """
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params or ())
            affected = cursor.rowcount
            connection.commit()
            return None if affected is None or affected < 0 else affected
        except Exception:
            try:
                connection.rollback()
            except Exception:
                pass
            raise
        finally:
            connection.close()


def target():
    return TargetDB(endpoint_for("target"))


def source():
    return SourceDB(endpoint_for("source"))


def _pick(which):
    if str(which).lower().startswith("s"):
        return source()
    return target()


def _no_access(which, extra=""):
    """Why this database cannot be reached, as specifically as possible.

    "No database access configured" is a dead end - it tells an operator
    that something is missing without telling them what to do about it. If
    discovery got as far as a host, this says the host, the user, and the
    exact variable to set.
    """
    which = "source" if str(which).lower().startswith("s") else "target"
    found = endpoint_for(which)

    if found and found.problem:
        return found.problem + extra

    if found and found.usable:
        return f"the {which} database is configured and reachable"

    return (f"nothing is known about the {which} database yet. The agent "
            f"discovers it from the pipeline's own component configuration, "
            f"so this usually means the component has not been read yet, or "
            f"it records no connection URL. Set {which.upper()}_DB_URL to "
            f"point at it explicitly." + extra)


def _unconfigured(which, extra=""):
    return {"which": which, "configured": False, "reachable": None,
            "note": _no_access(which, extra)}


# ================================================================= READS

def db_probe(which="target"):
    """SELECT 1. Is the database actually back?

    The single question every refusal so far has turned on. Three answers,
    as everywhere else in this agent: yes, no, or "I could not check" -
    and the third must never quietly become one of the first two.
    """
    database = _pick(which)
    if not database.configured:
        return _unconfigured(which)

    try:
        database.scalar("SELECT 1")
    except Exception as error:
        return {"which": which, "configured": True, "reachable": False,
                "error": f"{type(error).__name__}: {error}",
                "note": f"the {which} database refused a connection: "
                        f"{_short(error)}. The cause has NOT cleared."}

    return {"which": which, "configured": True, "reachable": True,
            "note": f"the {which} database answered - it is up and accepting "
                    f"queries. This does not by itself mean the pipeline's "
                    f"specific problem is fixed."}


def db_table_exists(which, table):
    """Did somebody rename or drop it?"""
    database = _pick(which)
    if not database.configured:
        return _unconfigured(which)

    try:
        schema, _, name = str(table).rpartition(".")
        rows = database.query(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name = %s" + (" AND table_schema = %s" if schema else ""),
            (name, schema) if schema else (name,))
    except Exception as error:
        return {"which": which, "configured": True, "table": table,
                "exists": None, "error": f"{type(error).__name__}: {error}",
                "note": f"could not check whether {table} exists: "
                        f"{_short(error)}"}

    if rows:
        found = f"{rows[0]['table_schema']}.{rows[0]['table_name']}"
        return {
            "which": which, "configured": True, "table": table,
            "exists": True, "resolved": found,
            # This note is longer than it looks like it needs to be because
            # the short version was read as a clean bill of health. A
            # column had been dropped from a target; the table was still
            # there; this said "exists"; the agent stopped looking and put
            # the fault in the source.
            "note": f"{found} exists on the {which}. THAT IS ALL THIS "
                    f"MEANS. It is not evidence that the {which} is "
                    f"healthy and it is not evidence that writes will "
                    f"succeed - a dropped column leaves the table exactly "
                    f"where it was while every write carrying that column "
                    f"fails. Existence and schema are different questions: "
                    f"call db_compare_schema before treating this end as "
                    f"sound.",
        }

    return {"which": which, "configured": True, "table": table,
            "exists": False,
            "note": f"{table} does NOT exist on the {which} database. If the "
                    f"writer expects it, that is the fault - name the table."}


def db_row_count(which, table):
    """How much is actually there, as opposed to how much Striim counted."""
    database = _pick(which)
    if not database.configured:
        return _unconfigured(which)

    try:
        name = qualify(table)
        count = database.scalar(f"SELECT COUNT(*) FROM {name}")
    except Exception as error:
        return {"which": which, "configured": True, "table": table,
                "rows": None, "error": f"{type(error).__name__}: {error}",
                "note": f"could not count {table}: {_short(error)}"}

    return {"which": which, "configured": True, "table": table,
            "rows": int(count or 0),
            "note": f"{table} holds {int(count or 0):,} rows on the {which}"}


def db_describe_table(which, table):
    """Columns, types, nullability, identity, and the primary key."""
    database = _pick(which)
    if not database.configured:
        return _unconfigured(which)

    schema, _, name = str(table).rpartition(".")
    schema = schema or "public"

    try:
        # The length and precision columns are not decoration. A varchar(3)
        # and a varchar(255) both report data_type "character varying", so
        # without these a type mismatch is invisible - and reading the
        # length back out of an error message ("value too long for
        # character varying(3)") is exactly the derivation the DDL rules
        # forbid, which is what stopped a widening repair from executing.
        columns = database.query(
            "SELECT column_name, data_type, is_nullable, column_default, "
            "       is_identity, identity_generation, "
            "       character_maximum_length, numeric_precision, "
            "       numeric_scale "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position", (schema, name))
        keys = database.query(
            "SELECT c.column_name FROM information_schema.table_constraints t "
            "JOIN information_schema.constraint_column_usage c "
            "  ON c.constraint_name = t.constraint_name "
            " AND c.table_schema = t.table_schema "
            "WHERE t.constraint_type = 'PRIMARY KEY' "
            "  AND t.table_schema = %s AND t.table_name = %s", (schema, name))
    except Exception as error:
        return {"which": which, "configured": True, "table": table,
                "columns": None, "error": f"{type(error).__name__}: {error}",
                "note": f"could not describe {table}: {_short(error)}"}

    if not columns:
        return {"which": which, "configured": True, "table": table,
                "columns": [], "exists": False,
                "note": f"{schema}.{name} has no columns in "
                        f"information_schema, which means it does not exist "
                        f"on the {which} database."}

    # A key row with no column name is not a key column. Filtering here
    # rather than trusting the query keeps every caller below - the
    # backfill, the DDL capture - from having to defend against it.
    primary_key = [k["column_name"] for k in keys if k.get("column_name")]

    # The declared type, as a person would write it: varchar(3), not
    # "character varying" with the 3 in a separate field nobody reads.
    for column in columns:
        column["declared_type"] = declared_type(column)

    # A column declared GENERATED ALWAYS refuses an explicit value, which
    # is a rejection cause that looks nothing like its symptom.
    always = [c["column_name"] for c in columns
              if c.get("identity_generation") == "ALWAYS"]

    note = f"{schema}.{name}: {len(columns)} columns"
    note += (f", primary key ({', '.join(primary_key)})" if primary_key
             else ", NO PRIMARY KEY - nothing can deduplicate this table")
    if always:
        note += (f". {', '.join(always)} is GENERATED ALWAYS and will refuse "
                 f"any explicit value the pipeline sends")

    return {"which": which, "configured": True, "table": table,
            "exists": True, "columns": columns,
            "primary_key": primary_key,
            "generated_always": always,
            "note": note}


def declared_type(column):
    """`character varying` + length 3 -> `varchar(3)`.

    information_schema splits a type across three fields and every one of
    them has to be read to know what the column actually is. Putting them
    back together here means nothing downstream has to remember to.
    """
    base = str(column.get("data_type") or "").strip()
    length = column.get("character_maximum_length")
    precision = column.get("numeric_precision")
    scale = column.get("numeric_scale")

    short = {"character varying": "varchar", "character": "char"}.get(base, base)

    if length:
        return f"{short}({length})"
    if base == "numeric" and precision:
        return f"numeric({precision},{scale or 0})"
    return short


def _constraints(which, table):
    """Every constraint on a table, with its definition."""
    database = _pick(which)
    schema, _, name = str(table).rpartition(".")
    return database.query(
        "SELECT c.conname AS name, c.contype AS kind, "
        "       pg_get_constraintdef(c.oid) AS definition "
        "FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE t.relname = %s AND n.nspname = %s",
        (name, schema or "public"))


_CONSTRAINT_KIND = {"c": "CHECK", "u": "UNIQUE", "p": "PRIMARY KEY",
                    "f": "FOREIGN KEY", "x": "EXCLUSION"}


def db_compare_constraints(source_table, target_table):
    """Constraints the TARGET enforces that the SOURCE does not.

    The quiet one. A constraint the source has never heard of will refuse
    rows the source considers perfectly valid - and the pipeline keeps
    RUNNING while they go into the exception store. Every status check
    passes, no alert fires, and the data is simply not there.

    Compared by DEFINITION rather than by name, because the same rule
    written under two different names is still the same rule.
    """
    try:
        theirs = _constraints("source", source_table)
        ours = _constraints("target", target_table)
    except Exception as error:
        return {"source_table": source_table, "target_table": target_table,
                "checked": False, "matches": None,
                "error": f"{type(error).__name__}: {error}",
                "note": f"the constraints were NOT compared "
                        f"({_short(error)}). This is 'I could not check', "
                        f"not 'they match'."}

    def shape(rows):
        return {str(r.get("definition", "")).strip().lower(): r for r in rows}

    source_defs, target_defs = shape(theirs), shape(ours)
    extra = [target_defs[d] for d in target_defs if d not in source_defs]

    # A primary key on the target that the source lacks is ordinary - the
    # target is usually the one with the key. What refuses rows is a CHECK
    # or a UNIQUE or a foreign key the source never validated against.
    rejecting = [c for c in extra if c.get("kind") in ("c", "u", "f")]

    result = {
        "source_table": source_table, "target_table": target_table,
        "checked": True, "matches": not rejecting,
        "only_on_target": [
            {"name": c["name"], "kind": _CONSTRAINT_KIND.get(c.get("kind"),
                                                             c.get("kind")),
             "definition": c["definition"]} for c in extra],
        "rejecting": [c["name"] for c in rejecting],
    }

    if not rejecting:
        result["note"] = (f"no constraint on {target_table} is missing from "
                          f"{source_table} in a way that could refuse rows")
        return result

    described = "; ".join(
        f"{c['name']} ({_CONSTRAINT_KIND.get(c.get('kind'), '?')}) "
        f"{c['definition']}" for c in rejecting)
    result["note"] = (
        f"{target_table} enforces {len(rejecting)} constraint(s) that "
        f"{source_table} does not: {described}. Any row the source considers "
        f"valid and this does not is REFUSED - and the pipeline keeps "
        f"running while those rows go to the exception store, so nothing "
        f"about its status will ever say so.")
    result["repair"] = [
        f'ALTER TABLE {qualify(target_table)} DROP CONSTRAINT "{c["name"]}";'
        for c in rejecting]
    return result


def db_check_privileges(which, table):
    """Can this user actually write here?

    THE SHARPEST GAP IN THE EVIDENCE CHAIN. A revoked INSERT leaves
    everything else looking perfect: the host answers, the table exists,
    the schema matches, `SELECT 1` succeeds. Every check the agent runs
    passes, and every write still fails - so the diagnosis eliminates the
    target and lands on the source, which is untouched and blameless.

    db_probe's own note already says a connection proves the host is up
    and NOT that the credentials work. This is the tool that closes that
    gap instead of leaving it as a caveat nobody acts on.
    """
    database = _pick(which)
    if not database.configured:
        return _unconfigured(which)

    wanted = ("SELECT", "INSERT", "UPDATE", "DELETE")
    try:
        name = qualify(table)
        # Aliased p_select / p_insert rather than select / insert, which
        # are reserved words and would need quoting to survive as labels.
        columns = ", ".join(f"has_table_privilege(%s, %s) AS p_{p.lower()}"
                            for p in wanted)
        rows = database.query(
            f"SELECT {columns}",
            tuple(value for p in wanted for value in (name, p)))
    except Exception as error:
        return {"which": which, "configured": True, "table": table,
                "privileges": None,
                "error": f"{type(error).__name__}: {error}",
                "note": f"could not read the privileges on {table} "
                        f"({_short(error)}). This is 'I could not check', "
                        f"not 'the permissions are fine'."}

    granted = ({p: bool(rows[0].get(f"p_{p.lower()}")) for p in wanted}
               if rows else {})
    missing = [p for p, ok in granted.items() if not ok]

    writes = [p for p in ("INSERT", "UPDATE", "DELETE") if p in missing]
    if writes:
        note = (f"the connected user cannot {', '.join(w.lower() for w in writes)} "
                f"on {table}. Every write the pipeline attempts fails on "
                f"permissions - the host is up, the table is there and the "
                f"schema is right, and none of that helps. THIS is the "
                f"fault; do not go looking at the source.")
    elif missing:
        note = (f"the connected user cannot {', '.join(missing).lower()} on "
                f"{table}, though it can write")
    else:
        note = f"the connected user holds SELECT, INSERT, UPDATE and DELETE on {table}"

    return {"which": which, "configured": True, "table": table,
            "privileges": granted, "missing": missing,
            "can_write": not writes, "note": note}


def grant_statement(table, privileges, user):
    """The exact GRANT a person must run. Never a description of one."""
    return (f"GRANT {', '.join(privileges)} ON {qualify(table)} "
            f"TO {qualify(user)};")


def db_replication_slot_status(slot=None):
    """Source only. The position a Postgres CDC pipeline actually resumes from.

    Striim's checkpoint is one half of the answer; the slot is the other,
    and they can disagree. A slot that is inactive while the application
    reports RUNNING is a fault nothing else in this agent detects - the
    pipeline looks healthy and is reading nothing.
    """
    database = source()
    if not database.configured:
        return _unconfigured("source",
                             " Without it the replication slot cannot be "
                             "checked, and the slot is where a Postgres CDC "
                             "pipeline's real position lives.")

    try:
        # pending_bytes is the whole difference between a pipeline that is
        # STALLED and one that is merely IDLE. It is how much write-ahead
        # log the server has produced that this slot has not consumed - so
        # zero means there is nothing to read and silence is correct,
        # while a large number means there is work sitting there going
        # undone. Without it, "nothing moved" cannot be told apart from
        # "nothing to move", and a quiet test pipeline looks broken.
        rows = database.query(
            "SELECT slot_name, plugin, slot_type, active, "
            "       restart_lsn::text AS restart_lsn, "
            "       confirmed_flush_lsn::text AS confirmed_flush_lsn, "
            "       pg_current_wal_lsn()::text AS server_lsn, "
            "       pg_wal_lsn_diff(pg_current_wal_lsn(), "
            "                       confirmed_flush_lsn)::bigint "
            "         AS pending_bytes "
            "FROM pg_replication_slots"
            + (" WHERE slot_name = %s" if slot else ""),
            (slot,) if slot else ())
    except Exception as error:
        return {"configured": True, "slots": None,
                "error": f"{type(error).__name__}: {error}",
                "note": f"could not read pg_replication_slots: "
                        f"{_short(error)}"}

    if not rows:
        missing = f"'{slot}'" if slot else "any"
        return {"configured": True, "slots": [], "exists": False,
                "note": f"there is no replication slot matching {missing} on "
                        f"the source. If the pipeline was reading through "
                        f"one, its position is gone: it cannot resume, and "
                        f"restarting means a fresh initial load."}

    inactive = [r["slot_name"] for r in rows if not r.get("active")]
    pending = max((r.get("pending_bytes") or 0) for r in rows)

    note = f"{len(rows)} replication slot(s): " + ", ".join(
        f"{r['slot_name']} ({r.get('plugin')}, "
        f"{'active' if r.get('active') else 'INACTIVE'}, "
        f"flushed to {r.get('confirmed_flush_lsn')})" for r in rows)

    if pending > 0:
        note += (f". {pending:,} bytes of write-ahead log sit between the "
                 f"server and this slot. THIS IS NOT BY ITSELF A BACKLOG: an "
                 f"idle server still advances its WAL for checkpoints and "
                 f"autovacuum, which a logical slot never consumes, so a "
                 f"healthy quiet pipeline reads exactly like this. Only the "
                 f"gap GROWING across checks means work is going undone")
        if inactive:
            note += (f". {', '.join(inactive)} is inactive, which is normal "
                     f"between transactions and is not on its own a fault")
    else:
        note += (". Nothing is waiting to be read - the source has produced "
                 "no changes this slot has not already consumed, so silence "
                 "here is a quiet source and not a stalled pipeline")
        if inactive:
            note += (f" ({', '.join(inactive)} is inactive, which is normal "
                     f"when there is nothing to stream)")

    # The two LSNs, so a CALLER WITH HISTORY can tell idle from stalled.
    # A single reading cannot: an idle Postgres still advances
    # pg_current_wal_lsn() for checkpoints, autovacuum and the background
    # writer, none of which a logical slot ever consumes - so a healthy
    # quiet pipeline sits permanently a few kilobytes "behind" and
    # pending_bytes > 0 is true almost always. Only the CHANGE in these two
    # between observations separates "the source moved and the slot did
    # not" from "neither moved".
    return {"configured": True, "slots": rows, "exists": True,
            "inactive": inactive,
            "pending_bytes": pending,
            "server_lsn": rows[0].get("server_lsn"),
            "confirmed_flush_lsn": rows[0].get("confirmed_flush_lsn"),
            "note": note}


def db_compare_schema(source_table, target_table):
    """Column-by-column, both ends. What the target is MISSING.

    `db_table_exists` returning true is not the same as the schema
    matching, and treating it as though it were is how a live fault got
    misread: a column was dropped from the target, the table still existed,
    the check passed, and the agent stopped looking. Every write was
    failing on a column that no longer had anywhere to go.

    A diff also gives the repair for free - the missing column's type comes
    from the source, so the ALTER can be written out exactly rather than
    guessed at.
    """
    right = db_describe_table("target", target_table)
    left = (db_describe_table("source", source_table) if source_table
            else {"columns": None,
                  "note": "the source table could not be determined from the "
                          "reader's configuration, so there is nothing to "
                          "compare the target against"})

    if not left.get("columns") or not right.get("columns"):
        # NOT COMPARED IS NOT MATCHED, and this is the one place that
        # distinction has already cost a wrong answer. A column was dropped
        # from a target, the comparison silently did not happen, and the
        # absence of a finding read as an absence of a problem - so the
        # fault was placed in the source, which the agent will not touch.
        #
        # `checked: False` makes the gap a fact the diagnosis has to
        # account for rather than a silence it can infer from.
        unreadable = [side for side, out in (("source", left), ("target", right))
                      if not out.get("columns")]
        return {
            "source_table": source_table, "target_table": target_table,
            "comparable": False, "checked": False, "matches": None,
            "source": left, "target": right,
            "target_columns": ({c["column_name"]: c["data_type"]
                                for c in right["columns"]}
                               if right.get("columns") else None),
            "note": f"THE SCHEMAS WERE NOT COMPARED - no column list from "
                    f"the {' or '.join(unreadable)}. "
                    f"{_ends_cleanly(left.get('note') or right.get('note'))}"
                    f"This is 'I could not check', NOT 'they match'. A missing "
                    f"column is invisible to an existence check, so do not "
                    f"conclude the target is healthy, and do not place the "
                    f"fault elsewhere on the strength of this.",
        }

    source_columns = {c["column_name"]: c for c in left["columns"]}
    target_columns = {c["column_name"]: c for c in right["columns"]}

    missing = [n for n in source_columns if n not in target_columns]
    extra = [n for n in target_columns if n not in source_columns]

    # Compare DECLARED types, so varchar(3) and varchar(255) are different
    # - which they are, and which data_type alone cannot see.
    differing = []
    for name, column in source_columns.items():
        other = target_columns.get(name)
        if not other:
            continue
        mine, theirs = declared_type(column), declared_type(other)
        if mine != theirs:
            differing.append({"column": name,
                              "source_type": mine, "target_type": theirs,
                              "widening": _widens(theirs, mine)})

    # Every extra target column, with what makes it a problem. A NOT NULL
    # column with no default that the source cannot supply rejects every
    # single write - and a source-to-target-only diff comes back clean.
    blocking = []
    for name in extra:
        column = target_columns[name]
        if (column.get("is_nullable") == "NO"
                and not column.get("column_default")
                and column.get("identity_generation") is None):
            blocking.append({"column": name,
                             "type": declared_type(column),
                             "why": "NOT NULL with no default, and the source "
                                    "sends no such column — every write is "
                                    "rejected"})

    result = {
        "source_table": source_table, "target_table": target_table,
        "comparable": True, "checked": True,
        "matches": not (missing or differing or blocking),
        "missing_in_target": missing,
        "extra_in_target": extra,
        "blocking_in_target": blocking,
        "differing_types": differing,
        "source_columns": {n: declared_type(c)
                           for n, c in source_columns.items()},
        "target_columns": {n: declared_type(c)
                           for n, c in target_columns.items()},
        "target_primary_key": right.get("primary_key"),
    }

    if result["matches"]:
        result["note"] = (f"the schemas match: {len(source_columns)} columns, "
                          f"same names and declared types on both sides")
        return result

    parts, repairs = [], []
    if missing:
        typed = ", ".join(f"{n} ({result['source_columns'][n]})"
                          for n in missing)
        parts.append(f"the target is MISSING {typed}")
        repairs += [_add_column_statement(target_table, n,
                                          result["source_columns"][n])
                    for n in missing]

    if differing:
        parts.append("; ".join(
            f"{d['column']} is {d['source_type']} on the source and "
            f"{d['target_type']} on the target" + _direction(d)
            for d in differing))
        # Only propose bringing the target UP to the source's type. Going
        # the other way narrows a column to fit a source that is already
        # smaller, which destroys capacity to fix nothing.
        repairs += [f'ALTER TABLE {qualify(target_table)} ALTER COLUMN '
                    f'"{d["column"]}" TYPE {d["source_type"]};'
                    for d in differing if d["widening"]]

    if blocking:
        parts.append("; ".join(
            f"the target has {b['column']} ({b['type']}) that the source "
            f"does not send, and it is {b['why']}" for b in blocking))
        # Dropping the constraint is preferred over dropping the column:
        # it loses nothing, and the column may be somebody else's.
        repairs += [f'ALTER TABLE {qualify(target_table)} ALTER COLUMN '
                    f'"{b["column"]}" DROP NOT NULL;' for b in blocking]

    if extra and not blocking:
        parts.append(f"the target has extra columns the source does not "
                     f"send: {', '.join(extra)} (all nullable or defaulted, "
                     f"so they accept writes)")

    result["note"] = (". ".join(parts) +
                      ". The table still exists and an existence check "
                      "passes, so none of this shows up as availability.")
    result["repair"] = repairs
    return result


def _direction(difference):
    """Which way the mismatch runs, and whether it is actually a problem.

    A target NARROWER than its source refuses or truncates values - that
    is the fault. A target WIDER than its source accepts everything it is
    sent and is nobody's emergency.
    """
    if difference["widening"] is True:
        return (" — the target is NARROWER, so values are being truncated or "
                "refused; widening it to match the source loses nothing")
    if difference["widening"] is False:
        return (" — the target is wider than the source, so it accepts "
                "everything sent; this is not what is rejecting writes")
    return " — the two types are not comparable, so treat any change as lossy"


def _widens(old, new):
    """Local wrapper so db.py does not import ddl.py at module level."""
    from .ddl import widens
    return widens(old, new)


def _ends_cleanly(text):
    """A borrowed note joined into a sentence, or nothing at all."""
    text = (text or "").strip()
    if not text:
        return ""
    return text + (" " if text.endswith((".", "!", "?")) else ". ")


def _add_column_statement(table, column, data_type):
    parts = [p.strip('"') for p in str(table).split(".") if p.strip()]
    if len(parts) == 1:
        parts = ["public"] + parts
    qualified = ".".join(f'"{p}"' for p in parts)
    return f'ALTER TABLE {qualified} ADD COLUMN "{column}" {data_type};'


def reconcile(source_table, target_table):
    """Counts on both sides, and the drift between them.

    This exists because of our own finding that Striim's in/out counters
    measure work performed rather than data present - 9,900 in and 9,900
    out reconciled perfectly while the target held about 1,900 rows. The
    only way to know what is actually there is to count it.
    """
    left = db_row_count("source", source_table)
    right = db_row_count("target", target_table)

    if left.get("rows") is None or right.get("rows") is None:
        missing = [side["which"] for side in (left, right)
                   if side.get("rows") is None]
        return {"source": left, "target": right, "drift": None,
                "note": f"cannot reconcile - no usable count from the "
                        f"{' and '.join(missing)}. This is 'I could not "
                        f"count', not 'the counts match'."}

    drift = left["rows"] - right["rows"]
    if drift == 0:
        verdict = "the two sides hold the same number of rows"
    elif drift > 0:
        verdict = (f"the target is missing {drift:,} rows that exist on the "
                   f"source")
    else:
        verdict = (f"the target holds {abs(drift):,} MORE rows than the "
                   f"source - which is what a silent re-read looks like")

    return {"source": left, "target": right, "drift": drift,
            "note": f"{source_table}: {left['rows']:,} rows. "
                    f"{target_table}: {right['rows']:,} rows. {verdict}."}


def db_config(striim, app):
    """What the agent worked out about this pipeline's databases.

        python -m agent tools db_config sarah_moolai.Traill

    Everything except the password, which is never printed, never logged
    and never returned - only the name of the variable it came from.
    """
    from . import striim_tools as st

    flow = st.get_flow_stats(striim, app)
    result = {"app": app, "endpoints": {}, "ready": [], "needs": []}

    for role in ("source", "target"):
        component = (flow.get(role) or {}).get("component")
        if not component:
            result["endpoints"][role] = {
                "role": role, "discovered_from": None, "usable": False,
                "note": f"no {role} component found for this app, so there "
                        f"is nothing to discover a connection from"}
            continue

        described = st.describe_component(striim, component)
        endpoint = remember(role, described)
        result["endpoints"][role] = endpoint.redacted()

        (result["ready"] if endpoint.usable else result["needs"]).append(role)

    if result["ready"] and not result["needs"]:
        result["note"] = (f"both ends are reachable: "
                          f"{', '.join(result['ready'])}")
    elif result["ready"]:
        result["note"] = (f"{', '.join(result['ready'])} reachable; "
                          f"{', '.join(result['needs'])} not yet — see the "
                          f"note on each")
    else:
        result["note"] = ("no database is reachable yet. Each endpoint below "
                          "says what is missing and how to supply it.")

    # The source stays read-only however it was discovered.
    result["source_is_read_only"] = True
    return result


def describe_config(result):
    """db_config as something worth printing at a terminal."""
    lines = [f"Databases for {result['app']}", ""]

    for role, found in result["endpoints"].items():
        lines.append(f"  {role.upper()}")
        if found.get("host"):
            lines.append(f"    host      {found['host']}:{found['port']}")
            lines.append(f"    database  {found['database']}")
            lines.append(f"    user      {found['user'] or '(none recorded)'}")
            if found.get("sslmode"):
                lines.append(f"    sslmode   {found['sslmode']}")
        lines.append(f"    password  {found.get('password', 'NOT SET')}")
        lines.append(f"    from      {found.get('discovered_from')}")
        if role == "source":
            lines.append("    access    READ ONLY, always")
        for line in str(found.get("note") or "").splitlines():
            lines.append(f"    {line}")
        lines.append("")

    lines.append(f"  {result['note']}")
    return "\n".join(lines)


def db_backfill_preview(source_table, target_table, column=None):
    """What a backfill would do. Writes nothing.

    Omit `column` to ask about whole missing ROWS - the question after a
    table has been recreated, when change capture will never deliver
    anything that happened before it was.

    Imported inside the function because `backfill` reads and writes
    through this module - the dependency runs one way at import time and
    the other way at call time, which is the only arrangement that does
    not deadlock the two.
    """
    from . import backfill
    return backfill.preview(source_table, target_table, column)


def _short(error):
    """The first line of a driver error. They are often a paragraph."""
    return str(error).strip().splitlines()[0][:200]


# ============================================================== REGISTRY

TOOLS = {
    "db_probe": db_probe,
    "db_table_exists": db_table_exists,
    "db_row_count": db_row_count,
    "db_describe_table": db_describe_table,
    "db_replication_slot_status": db_replication_slot_status,
    "db_compare_schema": db_compare_schema,
    "db_check_privileges": db_check_privileges,
    "db_compare_constraints": db_compare_constraints,
    "db_backfill_preview": db_backfill_preview,
    "reconcile": reconcile,
}
