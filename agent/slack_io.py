"""
Slack - the whole operator interface
====================================

Two directions.

OUTBOUND. One channel, one THREAD per incident. The alert is the parent;
diagnosis, action taken, verification and any human replies land underneath
it. Whoever scrolls in a day later sees the entire life of the incident in
one place instead of five loose messages in status order.

State is shown by an emoji on the parent - 🔴 raised, ✅ resolved. The agent
sets it, and because it is also a reaction a human can toggle it by hand.

INBOUND. Socket Mode, which opens an outbound WebSocket to Slack. No public
callback URL, no tunnel, and it runs from a laptop behind a router - which
is the entire reason inbound control is cheap enough to be worth building
for a one-day demo.

    monitor sarah_moolai.Trial5      start watching a pipeline
    unmonitor sarah_moolai.Trial5    stop watching it
    status                           what am I watching, and how is it
    diagnose sarah_moolai.Trial5     one-off investigation, no watching

There is no per-user auth. The agent holds one set of Striim credentials
and anyone in the channel is talking to that one operator identity. A
pipeline name in a message is just a string - which is why `monitor`
namespace-checks it before accepting.

DELIVERY MUST NEVER BE LOAD-BEARING. Every call here degrades to printing
on the terminal. An agent that stops diagnosing because Slack is having an
afternoon is worse than no agent.
"""

import re
import threading

from . import config
from .state import short_name

# The four states an incident can be in, shown as ONE reaction on the
# parent message. Exactly one is set at a time - moving state removes the
# old reaction before adding the new one, so the channel never shows an
# incident as simultaneously being worked on and resolved.
RAISED = "\U0001F534"       # red circle      - raised, nothing done yet
ACTING = "\U0001F527"       # wrench          - the agent is acting
RESOLVED = "✅"         # white check     - resolved and verified
NEEDS_HUMAN = "\U0001F64B"  # raising hand    - a person is needed

REACTION = {RAISED: "red_circle",
            ACTING: "wrench",
            RESOLVED: "white_check_mark",
            NEEDS_HUMAN: "raising_hand"}

# The one a human uses to say "I have dealt with this". We do not argue.
HUMAN_CLOSE = "white_check_mark"

# The only three things a thread reply may be. See Notifier.reply.
ALLOWED_REPLIES = {"action", "outcome", "failure"}
MAX_THREAD_REPLIES = 3


# ============================================================= OUTBOUND

class Notifier:
    """Posts incidents. Falls back to the terminal and carries on."""

    def __init__(self, token=None, channel=None, echo=True):
        self.channel = channel or config.SLACK_CHANNEL
        self.echo = echo
        self.client = None
        self._threads = {}
        self._local_counter = 0
        self._lock = threading.Lock()

        token = token or config.SLACK_BOT_TOKEN
        if token:
            try:
                from slack_sdk import WebClient
                self.client = WebClient(token=token)
            except Exception as error:      # missing package, bad token shape
                self._say(f"(Slack client unavailable: {error} - printing "
                          "messages here instead)")

    # ------------------------------------------------------------ basics

    def _say(self, text):
        if self.echo:
            print(text)

    @property
    def connected(self):
        return self.client is not None

    # ----------------------------------------------------------- posting

    def post_incident(self, text):
        """Start a thread. Returns a handle to reply into.

        The handle is truthy even with no Slack connection, so callers do
        not have to branch on whether delivery worked.
        """
        self._say("")
        self._say(text)

        if not self.client:
            with self._lock:
                self._local_counter += 1
                handle = f"local-{self._local_counter}"
            self._threads[handle] = {"channel": None, "ts": None, "text": text}
            return handle

        try:
            response = self.client.chat_postMessage(
                channel=self.channel, text=text, unfurl_links=False)
        except Exception as error:
            self._say(f"   (Slack delivery failed: {error})")
            with self._lock:
                self._local_counter += 1
                handle = f"local-{self._local_counter}"
            self._threads[handle] = {"channel": None, "ts": None, "text": text}
            return handle

        handle = response["ts"]
        self._threads[handle] = {"channel": response["channel"],
                                 "ts": response["ts"], "text": text,
                                 "state": None, "closed_by_human": False}
        self._react(handle, _leading_state(text) or RAISED)
        return handle

    def reply(self, handle, text, kind=None):
        """Add to an incident thread. THREE REPLIES, AND ONLY THREE KINDS.

        A thread reply is permitted for a state change the reader could
        not have predicted, and nothing else:

            action    "Running ALTER TABLE ..."
            outcome   "Recovered - 1,204 rows written"
            failure   "Restart did not hold - still HALT"

        Confidence scores, evidence lists, tool traces, round counts,
        checkpoint timestamps, "considered X but declined", notes about
        how the cluster is configured - none of those are state changes.
        If they matter they belong in the parent, and if they do not
        matter they belong nowhere. They went in the thread because the
        thread was the easy place to put them, and the thread became
        something nobody reads.

        This is enforced here rather than trusted to callers, because the
        rule is only worth having if it cannot be forgotten. Anything
        without an allowed kind, and anything past the third reply, goes
        to the terminal instead - where the full detail is kept anyway.
        """
        if kind not in ALLOWED_REPLIES:
            self._say(_indent(f"[not sent to Slack] {text}"))
            return

        if self.is_closed(handle):
            # A human marked this resolved. Do not argue with them, and do
            # not keep talking into a thread they have finished with.
            return

        record = self._threads.get(handle)
        if record is not None:
            sent = record.setdefault("sent", [])
            if len(sent) >= MAX_THREAD_REPLIES:
                self._say(_indent(f"[thread full] {text}"))
                return
            sent.append(text)

        self._say(_indent(text))

        if not self.client or not record or not record["ts"]:
            return

        try:
            self.client.chat_postMessage(
                channel=record["channel"], thread_ts=record["ts"],
                text=text, unfurl_links=False)
        except Exception as error:
            self._say(f"   (Slack reply failed: {error})")

    def set_state(self, handle, emoji):
        """Move the parent between 🔴 raised, 🔧 acting, ✅ done, 🙋 needs you.

        Does two things on purpose: rewrites the leading emoji so the text
        reads correctly in a notification preview, and moves the reaction,
        which is the part a human can toggle by hand.
        """
        record = self._threads.get(handle)
        if not record or record.get("closed_by_human"):
            return

        old = record["text"]
        for marker in REACTION:
            if old.startswith(marker):
                record["text"] = emoji + old[len(marker):]
                break

        if not self.client or not record["ts"]:
            record["state"] = emoji
            self._say(_indent(f"[{emoji}]"))
            return

        try:
            self.client.chat_update(channel=record["channel"],
                                    ts=record["ts"], text=record["text"])
        except Exception as error:
            self._say(f"   (Slack update failed: {error})")

        self._react(handle, emoji)

    def _react(self, handle, emoji):
        """Exactly one state reaction at a time - remove, then add."""
        record = self._threads.get(handle)
        name = REACTION.get(emoji)
        if not record or not name:
            return

        previous = record.get("state")
        record["state"] = emoji

        if not self.client or not record["ts"]:
            return

        if previous and previous != emoji and REACTION.get(previous):
            try:
                self.client.reactions_remove(channel=record["channel"],
                                             timestamp=record["ts"],
                                             name=REACTION[previous])
            except Exception:
                pass          # no_reaction and friends; not worth reporting

        try:
            self.client.reactions_add(channel=record["channel"],
                                      timestamp=record["ts"], name=name)
        except Exception:
            # already_reacted and friends. Not worth a line of output.
            pass

    def rewrite(self, handle, text):
        """Replace the parent message entirely.

        The incident is posted before the agent acts, so the thread exists
        for the action and the verification to reply into. That means the
        parent starts out provisional and has to be rewritten once the
        outcome is known - "working on it" becoming "fixed".

        WORTH KNOWING: chat_update does not re-notify. Whoever got the push
        for the provisional message never gets one for the rewrite; they
        see it only if they open Slack. So the provisional wording must
        stand on its own and must never claim a fix that has not happened.
        """
        record = self._threads.get(handle)
        if not record or record.get("closed_by_human"):
            return

        record["text"] = text
        self._say("")
        self._say(text)

        if not self.client or not record.get("ts"):
            return

        try:
            self.client.chat_update(channel=record["channel"],
                                    ts=record["ts"], text=text)
        except Exception as error:
            self._say(f"   (Slack rewrite failed: {error})")

    # ------------------------------------------------- human override

    def is_closed(self, handle):
        record = self._threads.get(handle)
        return bool(record and record.get("closed_by_human"))

    def close_by_human(self, timestamp):
        """Somebody put ✅ on an incident. Stop posting to it.

        Returns the handle if we recognised the message, else None. A human
        marking something resolved is a decision, not a data point - the
        agent does not remove their reaction, does not re-raise, and does
        not reply again in that thread.
        """
        record = self._threads.get(timestamp)
        if not record:
            return None
        record["closed_by_human"] = True
        record["state"] = RESOLVED
        self._say(_indent("(a human marked this resolved — standing down)"))
        return timestamp

    def say(self, text):
        """A message with no incident attached - command replies, banners."""
        self._say(text)
        if not self.client:
            return
        try:
            self.client.chat_postMessage(channel=self.channel, text=text)
        except Exception as error:
            self._say(f"   (Slack delivery failed: {error})")


def _indent(text):
    return "\n".join("    " + line for line in text.splitlines())


def _leading_state(text):
    for marker in REACTION:
        if text.startswith(marker):
            return marker
    return None


# ============================================================= COMPOSING

# The parent's emoji IS the incident state, so the text and the reaction
# never disagree. Tier 1 opens as raised and becomes 🔧 the moment it acts;
# tiers 2 and 3 open as 🙋 because that is already the answer.
TIER_STYLE = {
    1: (RAISED, "fixing it"),
    2: (NEEDS_HUMAN, "needs your OK"),
    3: (NEEDS_HUMAN, "needs you"),
}


# The four things an incident can be, and the words for each.
SHAPES = {
    "working":       (ACTING, "working on it"),
    "fixed":         (RESOLVED, "fixed"),
    "needs_command": (NEEDS_HUMAN, "needs one command from you"),
    "blocked":       (RAISED, "stopped, not safe to restart"),
    # A stalled pipeline has not stopped - it is running and moving
    # nothing, which is a different sentence and a worse problem. Saying
    # "stopped" about something whose status reads RUNNING makes the
    # message contradict the console a reader is looking at.
    "stalled":       (RAISED, "running, but moving nothing"),
    "failed":        (RAISED, "tried to fix it, it did not hold"),
    "unknown":       (NEEDS_HUMAN, "could not work out what is wrong"),
}

# Shapes whose label IS the news, so it stays as the headline. Everywhere
# else the headline names the fault and the label becomes a line inside.
TERMINAL_LABELS = {"fixed", "working", "unknown"}

# How the decision reads once it is no longer the title.
DECISIONS = {
    "blocked": "Not restarting",
    "stalled": "Not restarting",
    "failed": "Tried it, and it did not hold",
}


def compose_incident(app, verdict, record):
    """What broke → what I did → what you must do. Nothing else.

    TWO ARGUMENTS, TWO PROVENANCES, AND THEY DO NOT MIX.

      verdict  the model's prose. The headline and the plain-English cause.
      record   facts the CODE assembled: what it actually ran, what
               actually moved, what it is refusing to do and why.

    They are separate parameters rather than one merged dict because of a
    real failure. The old signature took the model's `human_action_needed`
    and the code's `next_step` in the same argument list, the model's field
    won, and the result reached an audience:

        ⚠ Don't restart: Inspect exception store rows to find rejection cause
        → Do this: Inspect exception store rows to find rejection cause

    Two identical lines, both telling a human to go and read something the
    agent could read itself. With the provenances split, a model string
    physically cannot land in the "what I did" or "what to run" slot.

    Everything diagnostic - confidence, evidence, the zone, the tool trace -
    belongs in a thread reply. The parent is what somebody reads on a phone
    at 3am, and none of that helps them decide anything.
    """
    shape = (record or {}).get("shape", "unknown")
    icon, label = SHAPES.get(shape, SHAPES["unknown"])
    name = short_name(app)
    record = record or {}

    # THE HEADLINE NAMES THE FAULT, NOT THE DECISION.
    #
    # It used to be the shape label, so two entirely different faults - a
    # dropped replication slot and a missing source file - both arrived as
    # "stopped, not safe to restart". Identical subject lines for unrelated
    # problems, and the one thing a reader needs to tell them apart was
    # computed by the model, put in `headline`, and then discarded.
    #
    # The decision is still visible: it is the emoji, and it is the line
    # that starts "Not restarting:". It just is not the title any more.
    fault = (verdict or {}).get("headline") if shape not in TERMINAL_LABELS else None
    lines = [f"{icon}  *{name}* — {(fault or label).rstrip('.')}", ""]

    cause = _sentence(verdict.get("what_happened") if verdict else None)
    if not cause and verdict and fault != verdict.get("headline"):
        cause = _sentence(verdict.get("headline"))
    lines.append(f"    {cause or record.get('blocked_reason') or ''}")

    if shape == "fixed":
        _extend(lines, _fixed_block(record))
    elif shape == "needs_command":
        _extend(lines, _command_block(record))
    elif shape in ("blocked", "failed", "stalled"):
        _extend(lines, _blocked_block(record, DECISIONS.get(shape)))
    elif shape == "working":
        _extend(lines, _working_block(record))
    else:
        _extend(lines, _unknown_block(record))

    return "\n".join(_drop_repeats(lines)).rstrip()


def _fixed_block(record):
    out = []
    did = record.get("did") or []
    if did:
        out += ["", f"    I did:  {_join(did)}"]
    if record.get("result"):
        out.append(f"    Result: {record['result']}")
    return out


def _command_block(record):
    command = record.get("command") or {}
    out = []

    if record.get("why_i_cannot"):
        out.append(f"    {record['why_i_cannot']}")

    where = command.get("where") or "the target database"
    out += ["", f"    Run this on {where}:", "",
            f"        {command.get('sql', '').strip()}", ""]

    if command.get("derived_from"):
        out.append(f"    {command['derived_from']}")
    out.append(f"    {command.get('next') or 'I will resume and verify once it is done.'}")
    if command.get("reversal"):
        out.append(f"    To undo it:  {command['reversal']}")
    return out


def _blocked_block(record, decision=None):
    out = []
    if record.get("blocked_reason"):
        reason = record["blocked_reason"]
        out += ["", (f"    {decision}: {reason}" if decision
                     else f"    {_capitalise(reason)}")]
    if record.get("did"):
        out.append(f"    I did:  {_join(record['did'])}")
    if record.get("next_action"):
        out += ["", f"    You need to: {record['next_action']}"]
    return out


def _working_block(record):
    out = []
    if record.get("plan_line"):
        out += ["", f"    {record['plan_line']}"]
    out.append("    I'm on it — I'll update this message when it's done.")
    return out


def _unknown_block(record):
    out = ["", f"    {_capitalise(record.get('blocked_reason') or '')}"]
    if record.get("next_action"):
        out += ["", f"    You need to: {record['next_action']}"]
    return out


def _sentence(text):
    text = (text or "").strip()
    if not text:
        return ""
    text = _capitalise(text)
    return text if text.endswith((".", "!", "?")) else text + "."


def _capitalise(text):
    """Guardrail reasons are written as clauses; on their own line they
    become sentences, and a sentence starts with a capital."""
    text = (text or "").strip()
    return text[:1].upper() + text[1:] if text else text


def _join(items):
    items = [str(i).strip().rstrip(".") for i in items if str(i).strip()]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", then ".join(items)


def _extend(lines, block):
    lines.extend(block)


def _drop_repeats(lines):
    """A line that repeats an earlier one is dropped, not printed twice.

    The old message managed to say "Inspect exception store rows" twice, in
    two slots that mean different things. A missing line beats a repeated
    one, so the later duplicate goes.
    """
    seen, kept = set(), []
    for line in lines:
        key = "".join(ch for ch in line.lower() if ch.isalnum())
        if not key:
            kept.append(line)
            continue
        if key in seen:
            continue
        seen.add(key)
        kept.append(line)
    return kept


def compose_detail(state):
    """Everything the parent deliberately does not say, for the thread."""
    verdict = state.get("verdict") or {}
    bits = []

    if verdict.get("confidence"):
        bits.append(f"{verdict['confidence']} confidence")
    if verdict.get("evidence"):
        bits.append(str(verdict["evidence"]))
    if verdict.get("fault_zone") and verdict["fault_zone"] != "unknown":
        bits.append(f"{verdict['fault_zone']} zone")
    if state.get("rounds"):
        rounds = state["rounds"]
        bits.append(f"{rounds} round" + ("" if rounds == 1 else "s"))
    if state.get("trace"):
        bits.append(f"{len(state['trace'])} checks")

    missing = (verdict.get("missing_information") or "").strip()
    if missing and missing.lower() not in ("none", "n/a", "-"):
        bits.append(f"couldn't check: {missing}")

    return "_" + " · ".join(bits) + "_" if bits else None


def compose_verification(app, outcome):
    """What the agent found when it went back to look."""
    name = short_name(app)
    recovered = outcome.get("recovered")

    if recovered is True:
        return f"{RESOLVED}  *{name}* recovered — {outcome['detail']}"
    if recovered is False:
        return (f"{NEEDS_HUMAN}  *{name}* the restart did NOT work — "
                f"{outcome['detail']}. Escalating: a person is needed.")
    # Neither confirmed nor denied. Say that, rather than picking one.
    return (f"{NEEDS_HUMAN}  *{name}* restarted, but {outcome['detail']} "
            f"Worth a look.")


# ============================================================== INBOUND

HELP = (
    "*Striim SRE agent*\n"
    "`monitor <ns.App>`     start watching a pipeline\n"
    "`unmonitor <ns.App>`   stop watching it\n"
    "`status`               what I'm watching, and how it is\n"
    "`diagnose <ns.App>`    one-off investigation, any namespace\n"
    "`help`                 this\n"
    "\n"
    "In an incident thread, when I have asked:\n"
    "`approve`              run the destructive repair I proposed\n"
    "`skip`                 leave it alone"
)

# Anything that looks like it was aimed at the agent but is not a command.
# Better a short help than silence: on stage, silence reads as broken.
ADDRESSED = re.compile(r"\b(monitor|unmonitor|watch|status|diagnos|agent|"
                       r"striim|pipeline|help)\w*", re.I)

def _clean_name(raw):
    """Strip Slack formatting and qualify a bare name.

    Slack passes backticks through as literal characters, and people type
    the short name. Both produce a silent no-op otherwise.
    """
    name = (raw or "").strip().strip("`").strip("<>").strip()
    if "." not in name:
        name = f"{config.NAMESPACE}.{name}"
    return name

class Console:
    """Inbound control over Socket Mode.

    `controller` is anything with add / remove / status / diagnose /
    human_closed. Keeping it a duck type means the tests drive the whole
    command surface without a Slack workspace.

    Every command answers immediately. The work behind `monitor` and
    `diagnose` can take a minute - a model call, then a verification wait -
    and a console that goes quiet for a minute looks broken, especially
    with an audience watching. So the reply is the acknowledgement, and the
    result arrives afterwards as an incident in the channel.
    """

    def __init__(self, controller, bot_token=None, app_token=None):
        self.controller = controller
        self.bot_token = bot_token or config.SLACK_BOT_TOKEN
        self.app_token = app_token or config.SLACK_APP_TOKEN
        self._handler = None

    @property
    def available(self):
        return bool(self.bot_token and self.app_token)

    # The command table is separate from the Slack plumbing so the tests can
    # call handle("monitor ns.App") directly.
    def handle(self, text, thread=None):
            text = (text or "").strip()

            # Approving a destructive repair happens IN the incident
            # thread, and the thread is what identifies WHICH repair is
            # being approved. The same word typed in the channel refers to
            # nothing, so it is left alone rather than guessed at.
            if thread:
                match = re.match(r"^(approve|yes|go ahead|do it)\b", text, re.I)
                if match:
                    return (self.controller.approve(thread)
                            or "There is nothing waiting for approval here.")

                match = re.match(r"^(skip|no|leave it|don'?t)\b", text, re.I)
                if match:
                    return (self.controller.skip(thread)
                            or "There is nothing waiting for approval here.")

            match = re.match(r"^monitor\s+(\S+)", text, re.I)
            if match:
                return self.controller.add(_clean_name(match.group(1)))

            match = re.match(r"^unmonitor\s+(\S+)", text, re.I)
            if match:
                return self.controller.remove(_clean_name(match.group(1)))

            match = re.match(r"^diagnose\s+(\S+)", text, re.I)
            if match:
                return self.controller.diagnose(_clean_name(match.group(1)))

            if re.match(r"^status\b", text, re.I):
                return self.controller.status()

            if re.match(r"^(help|\?)\s*$", text, re.I):
                return HELP

            # A bare verb with no pipeline name is the most common typo.
            if re.match(r"^(monitor|unmonitor|diagnose)\s*$", text, re.I):
                return (f"`{text.lower()}` needs a pipeline name, "
                        f"like `{text.lower()} {config.NAMESPACE}.Trial5`.")

            if ADDRESSED.search(text):
                return HELP

            return None      # ordinary channel chatter - stay out of it

    def start(self):
        """Open the socket and block. Returns False if it cannot."""
        if not self.available:
            return False

        from slack_bolt import App
        from slack_bolt.adapter.socket_mode import SocketModeHandler

        bolt = App(token=self.bot_token)

        @bolt.event("message")
        def on_message(event, say):
            if event.get("subtype") or event.get("bot_id"):
                return
            # thread_ts identifies which incident is being replied to, and
            # therefore which pending repair an `approve` refers to.
            thread = event.get("thread_ts")
            reply = self.handle(event.get("text", ""), thread=thread)
            if reply:
                say(reply, thread_ts=thread) if thread else say(reply)

        @bolt.event("app_mention")
        def on_mention(event, say):
            text = re.sub(r"<@[^>]+>", "", event.get("text", ""))
            thread = event.get("thread_ts")
            answer = self.handle(text, thread=thread) or HELP
            say(answer, thread_ts=thread) if thread else say(answer)

        @bolt.event("reaction_added")
        def on_reaction(event):
            # A human marking an incident resolved. Their call, not ours.
            if event.get("reaction") != HUMAN_CLOSE:
                return
            timestamp = (event.get("item") or {}).get("ts")
            if timestamp:
                self.controller.human_closed(timestamp)

        @bolt.event("reaction_removed")
        def on_reaction_removed(event):
            return          # subscribed so Bolt does not log it as unhandled

        self._handler = SocketModeHandler(bolt, self.app_token)
                # start() installs a SIGINT handler, and Python only permits that on
                # the main thread - this runs on a background thread, so it raises.
                # connect() opens the same socket without touching signals.
        self._handler.connect()
        threading.Event().wait()      # hold the thread open
        return True

    def start_in_background(self):
        if not self.available:
            return None
        thread = threading.Thread(target=self.start, daemon=True,
                                  name="slack-console")
        thread.start()
        return thread
