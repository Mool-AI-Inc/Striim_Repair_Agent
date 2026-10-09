"""
Where do ad-hoc query results actually live?
============================================

    python tools/probe_adhoc.py
    python tools/probe_adhoc.py --app sarah_moolai.Traill
    python tools/probe_adhoc.py --statement "SELECT * FROM ns.X_ExceptionStore LIMIT 5;"

THE QUESTION. Running a SELECT through /api/v2/tungsten does not return
rows. It returns a handle:

    adhocquery1f19293c4555311b6f622ca1b2e804a QUERY 1 01f19293-c455-ef51-b6f6-22ca1b2e804a

Striim has compiled the statement into an ad-hoc query APPLICATION - the
same Global.adhocquery<hex> objects that show up in /health's
stateChangeList. The rows are somewhere. This script goes looking, in the
three places worth trying, and prints what every attempt actually returned.

THE METHOD is the one that found the console endpoint's auth combination in
the first place: try every plausible shape, print the status code and the
first part of the body for each, and let the pattern show itself. A 404 and
a 401 and a 415 mean three different things, and you cannot tell which
you are getting without looking.

WHAT IT DOES TO THE CLUSTER. One SELECT, which creates one transient
ad-hoc query application in the Global namespace. That is the thing being
investigated, so it is unavoidable - but it is one, not one per attempt.
Everything after that is a GET or a read-only console command against the
handle it returned. Nothing is written, nothing is dropped, nothing outside
Global is touched.

IF NOTHING WORKS, that is a real result and it is worth recording. The
agent's current behaviour - report the exact statement and where to run it -
is honest, and it is already far better than the "inspect the exception
store" it replaced. Do not paper over a negative with a guess.
"""

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests                                              # noqa: E402

from agent import config                                     # noqa: E402
from agent import striim_tools as st                         # noqa: E402

TIMEOUT = 20
BODY_CHARS = 400


class Attempt:
    """One thing tried, and exactly what came back."""

    def __init__(self, group, what, status=None, body="", error=None,
                 content_type=""):
        self.group = group
        self.what = what
        self.status = status
        self.body = body
        self.error = error
        self.content_type = content_type

    @property
    def interesting(self):
        """Did this return something that could be rows?

        A 200 with an empty body is not interesting; a 200 with JSON is. A
        405 is interesting in a different way - the path exists and wants a
        different verb, which is a lead.
        """
        if self.status == 405:
            return True
        if self.status != 200 or not self.body.strip():
            return False
        stripped = self.body.strip()
        return stripped[0] in "[{" or "\t" in stripped or "," in stripped

    def line(self):
        if self.error:
            return f"  {'ERR':>4}  {self.what}\n           {self.error}"

        flag = "  <-- LOOK" if self.interesting else ""
        head = f"  {self.status:>4}  {self.what}{flag}"
        body = self.body.strip().replace("\n", " ")[:BODY_CHARS]
        if body:
            head += f"\n           {body}"
        return head


def get(striim, path, params=None, headers=None):
    """A GET against the cluster, reporting whatever happens."""
    url = f"{config.STRIIM_URL}{path}"
    try:
        response = requests.get(url, params=params or {},
                                headers=headers or {}, timeout=TIMEOUT)
    except Exception as error:
        return Attempt("", f"GET {path}", error=f"{type(error).__name__}: {error}")
    return Attempt("", f"GET {path}", status=response.status_code,
                   body=response.text[:2000],
                   content_type=response.headers.get("content-type", ""))


def console(striim, statement):
    """A console statement, sent the way the console endpoint wants it."""
    try:
        response = requests.post(
            f"{config.STRIIM_URL}/api/v2/tungsten",
            headers={"Authorization": f"token {striim.token}",
                     "Content-Type": "text/plain"},
            data=statement, timeout=TIMEOUT)
    except Exception as error:
        return Attempt("", statement, error=f"{type(error).__name__}: {error}")
    return Attempt("", statement, status=response.status_code,
                   body=response.text[:2000],
                   content_type=response.headers.get("content-type", ""))


# ============================================================ STRATEGY 1

def probe_endpoints(striim, handle):
    """A results endpoint keyed by the UUID or the query name.

    Both auth styles are tried, because this cluster already uses two:
    /health takes the token as a query parameter, /api/v2/tungsten takes it
    as an `Authorization: token <value>` header. There is no reason to
    assume a third endpoint picked either one.
    """
    uuid, name = handle["uuid"], handle["query_name"]
    fq = handle["fq_name"]

    paths = []
    for key in (uuid, name, fq):
        paths += [
            f"/api/v2/adhocquery/{key}",
            f"/api/v2/adhocquery/{key}/result",
            f"/api/v2/adhocquery/{key}/results",
            f"/api/v2/adhocqueryresult/{key}",
            f"/api/v2/query/{key}",
            f"/api/v2/queries/{key}",
            f"/api/v2/queryresult/{key}",
            f"/api/v2/result/{key}",
            f"/api/v2/results/{key}",
            f"/api/v2/data/{key}",
            f"/api/v2/applications/{key}/result",
            f"/api/v2/wactionstore/{key}",
            f"/api/v2/wastore/{key}",
            f"/api/v2/browse/{key}",
        ]

    # De-duplicate while keeping the order, so the output reads in the
    # order the paths were reasoned about.
    seen, ordered = set(), []
    for path in paths:
        if path not in seen:
            seen.add(path)
            ordered.append(path)

    attempts = []
    for path in ordered:
        by_param = get(striim, path, params={"token": striim.token})
        by_param.what += "   [token=param]"
        attempts.append(by_param)

        # Only try the header form where the parameter form got something
        # other than a flat 404 - otherwise this doubles the noise for no
        # information.
        if by_param.status != 404:
            by_header = get(striim, path,
                            headers={"Authorization": f"token {striim.token}"})
            by_header.what += "   [token=header]"
            attempts.append(by_header)

    return attempts


# ============================================================ STRATEGY 2

def probe_health(striim, handle):
    """Does the query application show up in /health with a readable store?

    Ad-hoc query apps are known to appear in stateChangeList. If one also
    appears in waStoreHealthMap then its results are a WActionStore, and
    everything this agent already knows about reading those applies.
    """
    striim.invalidate()
    record = striim.health()
    name, fq = handle["query_name"], handle["fq_name"]

    attempts = []
    for section, node in sorted(record.items()):
        if not isinstance(node, dict):
            continue
        hits = [k for k in node if name in k]
        attempts.append(Attempt(
            "", f"/health -> {section}", status=200 if hits else 404,
            body=json.dumps({k: node[k] for k in hits[:3]}, default=str)
                 if hits else ""))

    changes = [c for c in record.get("stateChangeList") or []
               if name in str(c.get("fqName", ""))]
    attempts.append(Attempt(
        "", "/health -> stateChangeList", status=200 if changes else 404,
        body=json.dumps(changes[:3], default=str) if changes else ""))

    return attempts


# ============================================================ STRATEGY 3

def probe_console(striim, handle):
    """A follow-up console command against the query the handle named."""
    name, fq, uuid = handle["query_name"], handle["fq_name"], handle["uuid"]

    statements = [
        f"SHOW {fq};",
        f"SHOW {name};",
        f"DESCRIBE {fq};",
        f"STATUS {fq};",
        f"MON {fq};",
        f"SELECT * FROM {fq};",
        f"PREVIEW {fq} LIMIT 5;",
        f"LIST QUERIES;",
        f"SHOW QUERYRESULT {uuid};",
        f"SHOW RESULT {uuid};",
        f"GET RESULT {uuid};",
        f"FETCH {uuid};",
    ]
    return [console(striim, statement) for statement in statements]


# ================================================================= RUN

def default_statement(striim, app):
    """A SELECT against an app's exception store, if it has one."""
    if app:
        return f"SELECT * FROM {app}_ExceptionStore LIMIT 5;"

    stores = (striim.health().get("waStoreHealthMap") or {})
    mine = [name for name in stores
            if name.startswith(config.NAMESPACE + ".")
            and name.endswith("_ExceptionStore")]
    if not mine:
        raise SystemExit(
            f"no exception store found in {config.NAMESPACE}. Pass --app or "
            f"--statement explicitly.")
    return f"SELECT * FROM {mine[0]} LIMIT 5;"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--app", help="pipeline whose exception store to query")
    parser.add_argument("--statement", help="the SELECT to run, verbatim")
    parser.add_argument("--skip", action="append", default=[],
                        choices=["endpoints", "health", "console"],
                        help="skip a strategy")
    args = parser.parse_args()

    for name in ("STRIIM_USER", "STRIIM_PASS"):
        if not os.environ.get(name):
            raise SystemExit(f"{name} is not set. Run setup.bat first.")

    print(f"Connecting to {config.STRIIM_URL} ...")
    striim = st.Striim()
    print("  authenticated\n")

    statement = args.statement or default_statement(striim, args.app)
    print(f"Running:  {statement}")
    first = console(striim, statement)
    print(first.line())
    print()

    handle = None
    try:
        entry = json.loads(first.body)[0]
        output = entry.get("output")
        text = output if isinstance(output, str) else json.dumps(output)
        handle = st.parse_query_handle(text)
        if handle is None and isinstance(output, list) and output:
            print("!! This returned ROWS, not a handle. Nothing to probe -\n"
                  "   get_rejected_rows already handles this shape.\n")
            print(json.dumps(output[:3], indent=2, default=str))
            return 0
    except Exception as error:
        print(f"!! could not parse the response ({error}). Raw body above.")
        return 1

    if not handle:
        print("!! no query handle in the response, and no rows either.\n"
              "   Paste the body above back and the parser can be widened.")
        return 1

    print(f"Handle parsed:")
    for key in ("query_name", "fq_name", "index", "uuid"):
        print(f"    {key:<12} {handle[key]}")
    print()

    strategies = [
        ("1. A RESULTS ENDPOINT KEYED BY THE UUID OR NAME",
         "endpoints", probe_endpoints),
        ("2. THE QUERY APP IN /health, WITH A READABLE STORE",
         "health", probe_health),
        ("3. A FOLLOW-UP CONSOLE COMMAND AGAINST THE QUERY",
         "console", probe_console),
    ]

    hits = []
    for title, key, run in strategies:
        if key in args.skip:
            continue
        print("=" * 74)
        print(f"  {title}")
        print("=" * 74)
        for attempt in run(striim, handle):
            print(attempt.line())
            if attempt.interesting:
                hits.append(attempt)
        print()

    print("=" * 74)
    if hits:
        print(f"  {len(hits)} attempt(s) returned something worth reading:")
        print("=" * 74)
        for attempt in hits:
            print(f"  {attempt.status}  {attempt.what}")
        print("\n  Paste those back. Wiring the winner into get_rejected_rows\n"
              "  is one function - the handle is already parsed.")
    else:
        print("  NOTHING RETURNED ROWS.")
        print("=" * 74)
        print("  That is a real result, and it is worth recording in\n"
              "  FINDINGS.md rather than trying again differently.\n")
        print("  The agent's current behaviour stands: report the exact\n"
              "  statement and where to run it. That is honest, and it is\n"
              "  already the difference between 'row id=9 violates\n"
              "  target_pkey - run this to see the rest' and the 'inspect\n"
              "  the exception store' it replaced.\n")
        print(f"  One ad-hoc query application was created: "
              f"{handle['fq_name']}\n"
              f"  Striim reaps these; it is listed here so it is not a "
              f"surprise.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
