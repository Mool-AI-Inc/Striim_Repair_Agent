"""
What the agent is watching
==========================

A set of pipeline names in a JSON file. That is genuinely all it is - but
it is on disk rather than in memory because the alternative is that
restarting the agent silently stops it watching the thing someone asked it
to watch an hour ago, and nobody finds out until the next outage.

Names are namespace-checked on the way in. Monitoring implies acting, and
the agent may only act inside its own namespace - so accepting a name it
could never act on would be a promise it cannot keep. `diagnose` has no such
restriction: reading anything on the cluster is fine, and it is how the
agent proves it is not file-reader-specific.
"""

import json
import threading

from . import config


class Registry:
    def __init__(self, path=None, namespace=None):
        self.path = path or config.REGISTRY_FILE
        self.namespace = namespace or config.NAMESPACE
        self._lock = threading.Lock()
        self.apps = []
        self._load()

    def _load(self):
        try:
            with open(self.path) as handle:
                self.apps = list(dict.fromkeys(json.load(handle)))
        except (FileNotFoundError, json.JSONDecodeError, TypeError):
            self.apps = []

    def _save(self):
        config.ensure_state_dir()
        with open(self.path, "w") as handle:
            json.dump(self.apps, handle, indent=2)

    # ------------------------------------------------------------ checks

    def in_namespace(self, app):
        return app.startswith(self.namespace + ".")

    def why_not(self, app):
        """The refusal message, in words a person can act on. None if fine."""
        if "." not in app:
            return (f"`{app}` has no namespace. Use the full name, "
                    f"like `{self.namespace}.Trial5`.")
        if not self.in_namespace(app):
            return (f"`{app}` is outside `{self.namespace}`. I can *diagnose* "
                    f"any pipeline on the cluster — try `diagnose {app}` — "
                    f"but I will only watch and act on my own namespace.")
        return None

    # ------------------------------------------------------------ writes

    def add(self, app):
        problem = self.why_not(app)
        if problem:
            return False, problem

        with self._lock:
            if app in self.apps:
                return True, f"Already watching `{app}`."
            self.apps.append(app)
            self._save()
        return True, f"Now monitoring `{app}`."

    def remove(self, app):
        with self._lock:
            if app not in self.apps:
                return False, f"Not watching `{app}`."
            self.apps.remove(app)
            self._save()
        return True, f"Stopped monitoring `{app}`."

    def replace(self, apps):
        """Set the whole list at once, WITHOUT the namespace check.

        Deliberate, and only used by the CLI. `watch --namespace other_team`
        is an operator explicitly asking to supervise pipelines the agent
        cannot act on - which is a real and useful thing to do on a shared
        cluster, and perfectly safe because DECIDE and the action layer both
        refuse independently. The CLI says so on the way past. Inbound
        `monitor` from Slack goes through add(), which does check.
        """
        with self._lock:
            self.apps = list(dict.fromkeys(apps))
            self._save()
        return self.apps

    # ------------------------------------------------------------- reads

    def all(self):
        with self._lock:
            return list(self.apps)

    def __len__(self):
        return len(self.apps)

    def __contains__(self, app):
        return app in self.apps
