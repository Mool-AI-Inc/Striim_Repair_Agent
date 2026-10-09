"""
One place that knows what tools exist
=====================================

There are two registries, and there has to be: Striim tools read the
cluster through a client, database tools open their own connection from
the environment. Keeping them apart is what makes "this tool can reach the
customer's database" a visible property of a tool rather than a detail
buried inside one.

What there must NOT be is two places that know how to CALL them. There
were - the diagnosis loop and the `tools` CLI command - and only one of
them was taught about the database registry. The result was that
`reconcile` existed, had a schema, was offered to the model, and answered
"No such tool" from the command line. It is the tool that answers this
project's own central finding, and it was unreachable by the only person
who would think to run it by hand.

So: one merged view, one dispatcher, one list. Adding a tool means adding
it to its registry and writing a schema, and the drift tests below fail if
either is missed.
"""

from . import db
from . import prompts
from . import striim_tools as st


def names():
    """Every callable tool, in a stable order."""
    return sorted(set(st.TOOLS) | set(db.TOOLS))


def needs_striim(name):
    """Does this tool read the cluster, or open its own connection?"""
    return name in st.TOOLS


def call(name, striim, *args, **kwargs):
    """Run one tool, whichever registry it lives in.

    Positional arguments are accepted for the command line, where they
    arrive as a list of strings; the model always uses keywords.
    """
    if name in st.TOOLS:
        return st.TOOLS[name](striim, *args, **kwargs)
    if name in db.TOOLS:
        return db.TOOLS[name](*args, **kwargs)
    raise KeyError(name)


def schemas():
    return prompts.TOOL_SCHEMAS


def described():
    """name -> one-line description, for `--help` style listings."""
    by_name = {s["name"]: s.get("description", "") for s in prompts.TOOL_SCHEMAS}
    return {name: by_name.get(name, "") for name in names()}
