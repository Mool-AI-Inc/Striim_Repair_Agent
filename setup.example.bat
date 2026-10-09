@echo off
REM ===========================================================
REM  Striim SRE agent - environment template
REM
REM  Copy to setup.bat, fill in the real values, then run it in
REM  the shell you are going to start the agent from:
REM
REM      copy setup.example.bat setup.bat
REM      setup.bat
REM      python -m agent monitor sarah_moolai.Trial5
REM
REM  setup.bat is gitignored. Never put real secrets in this file.
REM ===========================================================

REM ---- Striim -----------------------------------------------
set STRIIM_URL=https://developer.striim.com
set STRIIM_USER=your_striim_username
set STRIIM_PASS=your_striim_password

REM The namespace the agent is allowed to ACT on. It may read any
REM pipeline on the cluster; it may only write inside this one.
set STRIIM_NAMESPACE=sarah_moolai

REM Safety catch. Unset or 0 = dry run: every write is printed, none
REM is sent. Set to 1 only when you intend the agent to act for real.
set STRIIM_LIVE=

REM ---- Model ------------------------------------------------
set ANTHROPIC_API_KEY=sk-ant-...
set STRIIM_AGENT_MODEL=claude-sonnet-5

REM ---- Slack ------------------------------------------------
REM Socket Mode needs BOTH tokens. Outbound-only works with just the
REM bot token. Neither set = messages are printed to the terminal and
REM the agent carries on regardless.
REM   bot token   xoxb-  scopes: chat:write, reactions:write
REM   app token   xapp-  scope:  connections:write
set SLACK_BOT_TOKEN=xoxb-...
set SLACK_APP_TOKEN=xapp-...
set SLACK_CHANNEL=#striim-incidents

REM ---- Databases --------------------------------------------
REM YOU USUALLY DO NOT NEED A CONNECTION URL. The agent discovers
REM host, port, database and username from the pipeline's own
REM component configuration - Striim already knows them.
REM
REM The one thing it cannot discover is the password: Striim stores
REM that as an encrypted object handle, not a password, and that is
REM the right design. Supply it keyed by host:
REM
REM   set STRIIM_DBPASS_EP_SPRING_MOUSE_AXHFUX81_C_4_US_EAST_2_AWS_NEON_TECH=...
REM
REM ...or one password for everything:
set STRIIM_DBPASS_DEFAULT=

REM To find the exact variable name for a pipeline, ask:
REM   python -m agent tools db_config sarah_moolai.Traill
REM
REM The agent has full execution rights on the TARGET except DDL.
REM The SOURCE is READ ONLY, always - there is no write path for it
REM anywhere in the package, enforced in three independent layers.
REM
REM These two override discovery entirely, and are rarely needed.
set TARGET_DB_URL=
set SOURCE_DB_URL=
set DB_CONNECT_TIMEOUT=5
set DB_STATEMENT_TIMEOUT_MS=15000

REM ---- Target repair (see PLAN.md section 5 zone 2) ----------
set TARGET_MAX_ROWS=1000

REM The dedupe and partial-batch skills need these to bound a statement.
REM Each skill DECLINES rather than guesses when its own is missing -
REM there is no safe default for "which column identifies a duplicate".
set TARGET_TABLE=
set TARGET_PRIMARY_KEY=
set TARGET_BATCH_COLUMN=

REM Above this share of throughput, a recurring exception is systemic
REM rather than an isolated bad row, and the agent will not propose
REM ignoring it. 0.01 = 1%%.
set IGNORABLE_MAX_RATIO=0.01

REM ---- Reachability probe (optional) -------------------------
REM "Is the dependency back?" For file sources the configured path is
REM relative to the STRIIM SERVER, not to this machine. Point this at the
REM upload directory if it is mounted locally; without it the agent
REM reports "cannot check" rather than guessing the file is missing.
set STRIIM_FILE_ROOT=
set STRIIM_PROBE_TIMEOUT=5
