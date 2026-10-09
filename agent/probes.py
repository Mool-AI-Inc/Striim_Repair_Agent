"""
Is the dependency actually back?
================================

Nothing else in this agent can answer that, and it is the question every
"should I restart it?" turns on. Striim reports what its own components are
doing; it does not reach out and check whether the thing they were talking
to has come back.

THREE ANSWERS, NEVER TWO:

    True   probed it, it responded
    False  probed it, it did not respond
    None   could not probe it at all

That last one is the whole discipline of this file. "I could not check"
must never collapse into "it is down", because a skill that treats an
unprobeable dependency as a failure will decline every repair on every
connector it does not understand, and one that treats it as healthy will
restart into a target that is still on fire. Both are worse than saying so.

RULES, all enforced below:

  * read-only, always. A TCP connect and a stat. Never a query, never a
    schema read, never a write.
  * short timeout, so a hung host cannot stall the monitoring loop.
  * credentials come from the environment, never extracted from Striim's
    stored configuration. We do not mine the platform for secrets.

The TCP probe is deliberately shallow. Opening a socket to host:port proves
the machine is up and something is listening, which is exactly the question
"has the outage cleared?" - and it needs no credentials, so it works on
every database and API connector without configuration. It cannot tell you
whether a password still works. It does not pretend to.
"""

import os
import re
import socket
from pathlib import Path
from urllib.parse import urlparse

_HOSTNAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")

TIMEOUT_SECONDS = float(os.environ.get("STRIIM_PROBE_TIMEOUT", "5"))

# Default ports, so a connection string with no port is still probeable.
DEFAULT_PORTS = {
    "oracle": 1521, "thin": 1521,
    "postgresql": 5432, "postgres": 5432,
    "mysql": 3306, "mariadb": 3306,
    "sqlserver": 1433, "mssql": 1433,
    "mongodb": 27017,
    "kafka": 9092,
    "https": 443, "http": 80,
    "sftp": 22, "ftp": 21,
}


def _unknown(component, why):
    return {"component": component, "reachable": None, "kind": "none",
            "target": None, "detail": why}


def check_dependency(described):
    """Probe whatever `described` says this component talks to.

    Takes the output of striim_tools.describe_component - not a component
    name - because the endpoint lives in its configuration and this module
    does no Striim calls of its own.
    """
    if not described or described.get("error"):
        return _unknown(described.get("component") if described else None,
                        "the component configuration could not be read, so "
                        "there is nothing to probe")

    component = described.get("component")
    props = {str(k).lower(): v for k, v in (described.get("properties") or {}).items()}
    behaviour = (described.get("profile") or {}).get("behaviour")

    # --- file-like sources ------------------------------------------- #
    path = described.get("expects")
    if behaviour in ("file_reader", "append_only_writer") and path and not _looks_like_url(path):
        return _check_path(component, path)

    # --- anything with a host in its configuration -------------------- #
    endpoint = _endpoint_from(props)
    if endpoint:
        return _check_tcp(component, *endpoint)

    return _unknown(component,
                    f"no reachable endpoint is recorded in this component's "
                    f"configuration, so it cannot be probed from here"
                    + (f" (connector type: {behaviour})" if behaviour else ""))


# ==================================================================== FILE

def _looks_like_url(value):
    return "://" in str(value)


def _check_path(component, path):
    """Does the configured file exist and can it be read?

    The catch, and it is a real one: this path is relative to the STRIIM
    SERVER, not to us. `UploadedFiles/sarah_moolai/AI.csv` means something
    on the cluster and usually nothing here. So a path that does not resolve
    locally is reported as UNPROBEABLE, not as missing - claiming a file is
    gone because it is not on our laptop would be the worst kind of
    confident wrong answer.

    It resolves when the agent runs on the Striim host, or with the upload
    directory mounted, and STRIIM_FILE_ROOT points at it.
    """
    root = os.environ.get("STRIIM_FILE_ROOT")
    candidates = [Path(path)]
    if root:
        candidates.insert(0, Path(root) / path)

    for candidate in candidates:
        try:
            if candidate.exists():
                readable = os.access(candidate, os.R_OK)
                size = candidate.stat().st_size if readable else None
                return {
                    "component": component, "kind": "file",
                    "target": str(candidate),
                    "reachable": bool(readable),
                    "detail": (f"{candidate} exists and is readable "
                               f"({size:,} bytes)" if readable else
                               f"{candidate} exists but cannot be read"),
                    "size_bytes": size,
                }
        except OSError:
            continue

    # Did any parent directory resolve? If the directory is visible and the
    # file is not, that IS a real negative - we looked in the right place.
    parent = Path(root) / Path(path).parent if root else Path(path).parent
    try:
        if str(parent) not in (".", "") and parent.is_dir():
            return {"component": component, "kind": "file",
                    "target": str(Path(parent) / Path(path).name),
                    "reachable": False,
                    "detail": f"{parent} is visible and does not contain "
                              f"{Path(path).name}"}
    except OSError:
        pass

    return _unknown(component,
                    f"'{path}' is a path on the Striim server and is not "
                    f"visible from here, so whether it exists cannot be "
                    f"checked. Set STRIIM_FILE_ROOT if the directory is "
                    f"mounted locally.")


# ===================================================================== TCP

HOST_KEYS = ("connectionurl", "connection_url", "url", "endpoint",
             "brokeraddress", "brokers", "bootstrapservers", "hostname",
             "host", "servername", "serviceurl", "instanceurl")


def _endpoint_from(props):
    """Pull a host and port out of whatever the connector calls it."""
    for key in HOST_KEYS:
        raw = props.get(key)
        if not raw:
            continue
        found = _parse_endpoint(str(raw))
        if found:
            port = props.get("port") or props.get("portnumber")
            host, parsed_port = found
            try:
                return host, int(port) if port else parsed_port
            except (TypeError, ValueError):
                return host, parsed_port
    return None


def _parse_endpoint(raw):
    """host, port from a JDBC URL, a plain URL, or host:port.

    Handles the shapes actually seen on the cluster, e.g.
    jdbc:oracle:thin:@ora-prod-01:1521/ORCL
    """
    raw = raw.strip().split(",")[0].strip()      # broker lists: take the first
    if not raw:
        return None

    scheme_hint = None

    if raw.lower().startswith("jdbc:"):
        parts = raw.split(":")
        scheme_hint = parts[1].lower() if len(parts) > 1 else None
        _, _, remainder = raw.partition("@")
        if not remainder:
            # jdbc:postgresql://host:5432/db
            _, _, remainder = raw.partition("//")
        raw = remainder or raw

    if "://" in raw:
        parsed = urlparse(raw)
        if parsed.hostname:
            scheme_hint = scheme_hint or parsed.scheme
            return parsed.hostname, parsed.port or DEFAULT_PORTS.get(
                (scheme_hint or "").lower())
        raw = parsed.path

    raw = raw.split("/")[0].split("?")[0].strip("@")
    if not raw:
        return None

    host, _, port = raw.rpartition(":")
    if host and port.isdigit():
        return host, int(port)

    # Hostnames contain hyphens far more often than not - ora-prod-01,
    # kafka-1 - so isalnum() is the wrong test and quietly returns "cannot
    # probe" for most real hosts.
    if _HOSTNAME.fullmatch(raw):
        return raw, DEFAULT_PORTS.get((scheme_hint or "").lower())
    return None


def _check_tcp(component, host, port):
    if not port:
        return _unknown(component,
                        f"{host} is recorded but with no port, and none is "
                        f"known for this connector type, so it cannot be "
                        f"probed")

    target = f"{host}:{port}"
    try:
        with socket.create_connection((host, port), TIMEOUT_SECONDS):
            return {"component": component, "kind": "tcp", "target": target,
                    "reachable": True,
                    "detail": f"{target} accepted a connection — the host is "
                              f"up and listening. This does NOT confirm the "
                              f"credentials still work."}
    except socket.timeout:
        return {"component": component, "kind": "tcp", "target": target,
                "reachable": False,
                "detail": f"{target} did not answer within "
                          f"{TIMEOUT_SECONDS:g}s"}
    except OSError as error:
        why = error.strerror or error
        return {"component": component, "kind": "tcp", "target": target,
                "reachable": False,
                "detail": f"{target} refused the connection ({why})"}


def describe_reachability(probe):
    """One plain line for an incident."""
    if not probe:
        return "no dependency was probed"
    if probe["reachable"] is True:
        return f"the dependency is back: {probe['detail']}"
    if probe["reachable"] is False:
        return f"the dependency is still down: {probe['detail']}"
    return f"could not check the dependency — {probe['detail']}"
