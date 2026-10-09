# Striim SRE Agent

This watches your Striim pipelines and fixes them when they break.

Striim already tells you *that* something broke. It never tells you *why*.
That gap — the twenty minutes at 3am where somebody logs in, runs `mon`, (# mon is command in striim pipeline expanding to monitor)
squints at a checkpoint and tries to work out whether restarting is safe —
is what this closes.

It reads Striim's control plane and both databases, works out what is
actually wrong, repairs what it safely can, and writes the rest up in
plain English for whoever is on call. When it can't fix something, it says
so and hands over the exact command to run.

The one rule everything else is built on: **the model decides nothing.**
It reads evidence and suggests. Every guardrail — what may be touched,
what may be restarted, what gets backed up first — lives in Python where
it can't be argued with. Several of the tests hand the model a
confidently wrong answer on purpose and check the code overrules it.

---

## What it does

### Watching

It polls the pipelines you tell it to watch. A healthy pipeline produces
nothing. So does one somebody stopped on purpose, and so does the same
fault twice in a row — a channel that chirps for no reason is a channel
people mute.

It also catches the fault nothing else on the platform can see: a pipeline
reporting RUNNING while its read position hasn't moved. Striim shows green,
checkpoints keep being written, and no data is going anywhere. The agent
checks whether there is actually work waiting (unread write-ahead log)
before calling that a stall, so an idle pipeline on a quiet source stays
quiet.

### Repairing

- If the fix is inside the pipeline or on the target, it does it.
- Adds a column the target is missing, widens one that's too small, removes
duplicate rows, restarts a reader that lost its grip on the replication
slot.
- Then it resumes the pipeline and checks that data actually moved —
not that the status says RUNNING, which is a different and much weaker
claim.

- After a structural repair it backfills.
- A column that was dropped and put back is empty for every row that existed before, and change capture only
carries changes forward 
- so it copies the values across from the source, in batches, matched on the primary key. 
- Same for a recreated table: the rows the pipeline never saw get filled in.

### Refusing

Some things it won't do, and this matters more than the repairs.

It never writes to the source. That's somebody else's system of record and
a wrong write there is unrecoverable.

It won't restart a pipeline where restarting would silently duplicate
everything — a file reader whose saved position points into a file that's
gone, say. No alert fires for a doubled table. You find out weeks later
when somebody queries a total.

Anything destructive on the target — dropping a column, narrowing a type,
dropping a constraint — copies the affected rows to a backup table first,
posts the statement and the row count in the thread, and waits for someone
to reply `approve`. If nobody answers in fifteen minutes it gives up and
says so.

### Warning

Separately from all that, it compares the source and target schemas on a
timer. If your schema evolution is set to Ignore, a change on the source
gets captured and deliberately dropped. Nothing breaks — the target just
falls behind, quietly, until some write needs the missing column hours
later.

That gets a yellow warning, not an incident. Nothing is broken and the
message says so.

---

## What it can and can't touch

| Where | What it may do |
|---|---|
| Striim control plane | read anything on the cluster; start, stop, resume and reconfigure only inside your namespace |
| Target database | full rights, including DDL — destructive changes need a reply in the thread |
| Source database | read only, always. There is no write path in the code |
| Exception store | reads whether rows were refused, how many, how recently |

### The repairs it knows

| Repair | Where | Runs on its own? |
|---|---|---|
| Resume a pipeline whose cause has cleared | pipeline | yes |
| Add a column the target is missing | target | yes |
| Widen a column that's too small | target | yes |
| Recreate a dropped table from the source schema | target | yes |
| Backfill missing rows or values from the source | target | yes |
| Remove duplicate rows left by a re-read | target | yes, once counted |
| Drop NOT NULL that's rejecting every write | target | needs `approve` |
| Drop a column, drop a table, narrow a type | target | needs `approve`, backs up first |
| Clear a half-written batch | target | proposes |
| Lower a batch policy that never flushes | pipeline | proposes |
| Ignore a recurring recoverable exception | pipeline | proposes — it discards rows, so a person decides |
| Anything wrong with the source | source | never acts, writes up exactly what to do |

---

## What's in the folder

### The agent

| File | What it's for |
|---|---|
| `graph.py` | the pipeline of steps — watch, triage, gather evidence, diagnose, decide, act, verify, close |
| `nodes.py` | what each of those steps actually does. The guardrails live here |
| `skills.py` | the named repairs and their preconditions |
| `striim_tools.py` | everything it reads from Striim. Read-only |
| `striim_actions.py` | the only file that changes anything in Striim |
| `db.py` | database access. Target read-write, source read-only |
| `ddl.py` | schema changes, and the backup-before-destroying rules |
| `backfill.py` | copying missing rows and values from source to target |
| `drift.py` | the scheduled schema comparison and its warnings |
| `target_repair.py` | corrective SQL against the target |
| `prompts.py` | what the model is told, and the tool descriptions |
| `slack_io.py` | incidents, threads, emoji, and the inbound commands |
| `supervisor.py` | the loop, and the approve/skip handling |
| `probes.py` | is the thing it depends on actually back? |
| `profiles.py` | how each connector type behaves when it goes quiet |
| `config.py` | every setting, one screen, all from the environment |
| `cli.py` | the command line |

Also `toolbox.py` (one registry so tools can't go missing), `state.py`,
`registry.py` (what's being watched, on disk), `capture.py`, `demo.py`.

### The docs

| File | What's in it |
|---|---|
| `FINDINGS.md` | **read this before touching the Striim integration.** Nineteen things Striim actually does, several of which contradict the documentation. Every one cost hours |
| `DEMO.md` | the eight-minute demo script, with an offline fallback |
| `PLAN.md`, `TASK-2.md`, `TASK-4.md`, `TASK-5.md` | the original briefs, kept for context |
| `tests/` | 226 tests, all offline, about four seconds |
| `tools/probe_adhoc.py` | a one-off script from working out where ad-hoc query results live |

### Where it keeps things

`state/` holds three files: what it's watching, what it's already tried
(so a restart doesn't reset the restart budget), and an append-only audit
log of everything it has ever changed. Don't delete the audit log.

---

## Setting it up

You need Python 3.10 or newer.

credentails are present in credentials.txt file.

**1. Install the dependencies**

```
pip install -r requirements.txt
```

**2. Make your settings file**

```
copy setup.example.bat setup.bat
```

Open `setup.bat` and fill it in. The example file explains each one. The
short version:

- `STRIIM_USER` / `STRIIM_PASS` — the same login you use for the web UI
- `STRIIM_NAMESPACE` — the only namespace it's allowed to change anything in
- `ANTHROPIC_API_KEY` — for the diagnosis step
- `SLACK_BOT_TOKEN` (`xoxb-`) and `SLACK_APP_TOKEN` (`xapp-`) — you need
  both for Slack commands to work. Bot scopes: `chat:write`,
  `reactions:write`. App token scope: `connections:write`
- `STRIIM_LIVE` — **leave this empty until you're ready.** Empty means it
  prints what it would do and sends nothing

`setup.bat` is gitignored. Don't commit it.

**3. Database passwords**

You don't need to write connection strings. The agent finds the host,
port, database and username from the pipeline's own configuration —
Striim already knows them.

The one thing it can't get is the password, because Striim stores that as
an encrypted object rather than text. Which is correct, and we don't try
to get around it. Give it the password keyed by host:

```
set STRIIM_DBPASS_EP_SPRING_MOUSE_AXHFUX81_C_4_US_EAST_2_AWS_NEON_TECH=...
```

Or one for everything, if that's how your setup works:

```
set STRIIM_DBPASS_DEFAULT=...
```

To find out the exact variable name for a pipeline, ask it:

```
python -m agent tools db_config sarah_moolai.sarah1_CDC
```

It'll print the host, database and username it found, and tell you which
variable to set. It never prints the password.

**4. Check everything connects**

```
setup.bat
python -m agent doctor
```

You want Striim connected, the model key present, and Slack saying
`inbound: Socket Mode`. If it says `OUTBOUND ONLY` the app token is
missing and Slack commands won't work — incidents will still post.

**5. Start it**

```
python -m agent slack
```

That opens the Slack connection and starts watching whatever's in
`state/registry.json`. It runs until you stop it with Ctrl-C.

---

## Using it day to day

Type these in the Slack channel:

```
monitor sarah_moolai.sarah1_CDC     start watching a pipeline
unmonitor sarah_moolai.sarah1_CDC   stop watching it
status                              what am I watching, and how is it
diagnose sarah_moolai.Trial5        look at one now, any namespace
help                                the list
```

Inside an incident thread, when it's asked for one:

```
approve     run the destructive repair it proposed
skip        leave it alone
```

If you put a ✅ on an incident yourself, it takes that as "I've got this"
and stops posting to that thread.

From the command line:

```
python -m agent doctor                       check every connection
python -m agent diagnose <ns.App>            one investigation, then exit
python -m agent drift <ns.App>               compare schemas now
python -m agent tools db_config <ns.App>     what databases it found
python -m agent monitor <ns.App> <ns.App>    watch specific pipelines
python -m agent demo                         the demo, offline
```

To run the tests:

```
python tests/test_acceptance.py
```

They need no cluster, no database and no API key. If they don't all pass,
something's wrong with the checkout.

---

## Things you should know before trusting it

I'd rather you heard these from the README than found them at 3am.

**It won't do anything until `STRIIM_LIVE=1`.** Until then every write is
printed instead of sent. Worth leaving it in dry run for a day and reading
what it would have done.

**Pending approvals don't survive a restart.** If it's waiting on an

`approve` and you restart the agent, that request quietly disappears.
There's a record in the audit log, but nobody gets told.

**Exception store rows can't be read over the API.** We tried three ways
and none worked — a console `SELECT` comes back as a query handle, not
data. So the agent tells you how many rows were refused and hands you the
exact statement to run in the Striim console yourself.

**Some `MON` output can't be parsed.** The response shape has never been
captured properly, so if the DDL counters come back in a format it doesn't
recognise it reports "I couldn't read this" rather than guessing zero.
That distinction matters — a zero it invented would hide the exact drift
it's meant to catch.

**It creates a temporary object when it reads the exception store.** A
console `SELECT` makes Striim compile a short-lived application in the
`Global` namespace. Harmless, but it's a write of sorts and it's outside
the namespace lock, which is why there's no general SQL tool.

---

## If something looks wrong

Start with `python -m agent doctor` — most problems are a missing
credential.

Every incident carries its reasoning in the thread: what it checked, what
it found, how confident it was. If a diagnosis looks wrong, the evidence
is there to check it against.

`state/audit.log` has every change it has ever made, with timestamps,
statements, row counts and backup table names. If it altered something and
you want it back, the restore statement is in there too.

And if it says it couldn't check something, believe it. The whole codebase
is built around the difference between "I looked and it's fine" and "I
couldn't look" — those are never reported as the same thing.
