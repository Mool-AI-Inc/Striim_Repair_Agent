"""
Connector behaviour profiles
============================

Striim talks to everything. The agent only talks to Striim. So the TOOL
layer does not grow when a new connector appears - the *knowledge* does,
and it lives here.

Profiles are keyed by BEHAVIOUR, not by product. One entry per product
would be forty entries, constant maintenance, and it would say nothing
useful anyway: what changes the diagnosis is how a connector behaves when
it goes quiet and what happens if you restart it. There are only eight
answers to that, and Oracle and SQL Server share one of them.

Two rules, both learned by getting them wrong first:

  * State the CONDITION, not the conclusion. A flat "restart_risk: HIGH"
    drowns out contradicting evidence from other tools - the model repeats
    the loudest input instead of reconciling it with the rest. During
    prototyping the file-reader profile said HIGH unconditionally, and the
    agent kept warning about duplication on a pipeline with a perfectly
    valid checkpoint. Name the tool that settles the question instead.

  * An unrecognised adapter gets an explicit "I do not know" profile,
    never a default. Quietly assuming file-reader behaviour for something
    that might be a CDC source is how an agent gives confident bad advice.
"""

# ==================================================== BEHAVIOUR CLASSES

BEHAVIOUR_PROFILES = {
    "cdc_reader": {
        "silence_is_normal": False,
        "why": "change capture on a live database - a busy source is rarely "
               "silent for long, so sustained silence is itself a symptom",
        "restart_risk": "resumes from the stored log position (SCN, LSN or "
                        "binlog offset) IF recovery is enabled - call "
                        "get_checkpoints. If the source log has been purged "
                        "past that position it cannot resume at all: that is "
                        "an escalation, not a restart.",
        "resolve_with": ["get_checkpoints", "describe_component"],
    },
    "batch_reader": {
        "silence_is_normal": True,
        "why": "runs a query, delivers the result, and finishes - silence "
               "after that is the normal end state, not a stall",
        "restart_risk": "may re-run the whole query and deliver every row "
                        "again. Check whether the target is append-only "
                        "before restarting; if it is, a re-run duplicates.",
        "resolve_with": ["get_checkpoints", "get_flow_stats"],
    },
    "file_reader": {
        "silence_is_normal": True,
        "why": "reads its input to the end, then idles with nothing to do",
        "restart_risk": "DEPENDS ON THE CHECKPOINT. Call get_checkpoints AND "
                        "describe_component on this component before judging. "
                        "If there is no usable position - or the recorded "
                        "offset is implausible for the file that is actually "
                        "there - a restart re-reads from the start and "
                        "appends every row to the target again. No alert "
                        "fires for that. If the position is sound, the reader "
                        "resumes from it and a restart is reasonably safe.",
        "resolve_with": ["get_checkpoints", "describe_component"],
    },
    "api_poller": {
        "silence_is_normal": True,
        "why": "polls a remote API on a schedule - quiet outside business "
               "hours is expected, and rate limits produce gaps too",
        "restart_risk": "may re-consume API call quota, and may re-deliver "
                        "the last polled window. Expired credentials and "
                        "revoked tokens are the usual cause here - a restart "
                        "will not fix either.",
        "resolve_with": ["get_errors", "describe_component"],
    },
    "spreadsheet_reader": {
        "silence_is_normal": True,
        "why": "reads a sheet that only changes when a person edits it",
        "restart_risk": "a whole-sheet re-read is likely, which duplicates "
                        "into an append-only target. Manual edits and "
                        "reordered columns break these without warning, and "
                        "restarting does not undo either.",
        "resolve_with": ["get_errors", "describe_component"],
    },
    "stream_reader": {
        "silence_is_normal": False,
        "why": "consumes a live stream - sustained silence is suspicious "
               "unless the producers are known to have stopped",
        "restart_risk": "resumes from the committed offset if there is one. "
                        "If retention has passed that offset the gap is "
                        "permanent and silent - data will be missing and "
                        "nothing will say so.",
        "resolve_with": ["get_checkpoints", "describe_component"],
    },
    "transactional_writer": {
        "silence_is_normal": None,
        "why": "a target; it writes when the source gives it something, so "
               "its silence describes the source, not itself",
        "restart_risk": "a batch interrupted mid-flight may be half applied. "
                        "This is the one place where corrective SQL against "
                        "the target is appropriate - but count the rows and "
                        "show the statement first.",
        "resolve_with": ["get_rejected_events", "get_errors"],
    },
    "append_only_writer": {
        "silence_is_normal": None,
        "why": "a target; it appends and never replaces",
        "restart_risk": "APPENDS, NEVER REPLACES. This is why a source "
                        "re-read duplicates silently: nothing here rejects a "
                        "row it has already seen, and no alert fires. Weigh "
                        "any restart against the source's read position.",
        "resolve_with": ["get_flow_stats", "describe_component"],
    },
}

UNRECOGNISED = {
    "silence_is_normal": None,
    "why": "I do not recognise this adapter, so I do not know whether "
           "silence is normal for it",
    "restart_risk": "unknown - I do not know how this connector behaves on "
                    "restart. Do not assume it is safe and do not assume it "
                    "is not. Call describe_component to see what it is "
                    "actually configured to do, and say plainly in the "
                    "diagnosis that the connector type is unrecognised.",
    "resolve_with": ["describe_component"],
}


# ======================================================= ADAPTER TABLES
#
# Striim names auto-generated components after their adapter -
# Trial5_FileReaderSource, X_OracleSource - so the type is usually sitting
# in the name. Shorter aliases are listed alongside the full adapter name
# because hand-named components often carry only the product.
#
# Matching is longest-token-first, so "filereader" beats a stray "file"
# and "googlesheetsreader" beats "googledrivereader".

READER_ADAPTERS = {
    # relational change capture
    "oraclereader": "cdc_reader", "ojet": "cdc_reader",
    "postgresqlreader": "cdc_reader", "postgresreader": "cdc_reader",
    "postgrescdc": "cdc_reader",
    "mysqlreader": "cdc_reader",
    "mariadbreader": "cdc_reader",
    "mssqlreader": "cdc_reader", "msjet": "cdc_reader",
    "sqlserverreader": "cdc_reader",
    "spannerreader": "cdc_reader",
    "mongodbreader": "cdc_reader", "cosmosdbreader": "cdc_reader",

    # batch / initial load / warehouse sources
    "databasereader": "batch_reader",
    "incrementalbatchreader": "batch_reader",
    "bigqueryreader": "batch_reader",
    "snowflakereader": "batch_reader",
    "databricksreader": "batch_reader",
    "fabricdatawarehousereader": "batch_reader",
    "biglakereader": "batch_reader",
    "icebergreader": "batch_reader",
    "redshiftreader": "batch_reader",

    # file-like
    "filereader": "file_reader",
    "s3reader": "file_reader",
    "sftpreader": "file_reader", "ftpreader": "file_reader",
    "onedrivereader": "file_reader",
    "googledrivereader": "file_reader",
    "gcsreader": "file_reader",
    "adlsreader": "file_reader",
    "azureblobreader": "file_reader",
    "hdfsreader": "file_reader",

    # API pollers
    "salesforcereader": "api_poller",
    "salesforceplatformeventreader": "api_poller",
    "salesforcepushtopicreader": "api_poller",
    "workdayreader": "api_poller",
    "zohocrmreader": "api_poller", "zohoreader": "api_poller",
    "jirareader": "api_poller",
    "googleanalyticsreader": "api_poller",
    "googleadsreader": "api_poller",
    "adobeanalyticsreader": "api_poller",
    "hubspotreader": "api_poller",
    "shopifyreader": "api_poller",
    "netsuitereader": "api_poller",
    "servicenowreader": "api_poller",
    "stripereader": "api_poller",

    # spreadsheets
    "googlesheetsreader": "spreadsheet_reader",
    "excelonlinereader": "spreadsheet_reader",

    # event streams
    "kafkareader": "stream_reader",
    "jmsreader": "stream_reader",
    "mqttreader": "stream_reader",
    "kinesisreader": "stream_reader",
    "eventhubreader": "stream_reader",
    "pubsubreader": "stream_reader",
}

# Product words, consulted ONLY after the full adapter names above have
# failed. Real component names on a shared cluster are far messier than the
# documentation suggests - observed in one capture of 576 pipelines:
#
#   sarah_moolai.Trial5_FileReaderSource          the documented shape
#   sarah_moolai.File_Trial5_Target               product first, no "Writer"
#   MANIMARAN22.OracleToFabricDWH_CDC_Target      no adapter word at all
#   harrybwatson.Vercel_to_Azure_SQL_CDC_PostgresCDC
#   guhan.MyNeoPipelineKafkaToS3_Source           product only, in the app name
#
# These are a second-best guess and are searched within the component's ROLE
# only - never across it. Matching "oracle" in a *target* name would call a
# Fabric warehouse writer a CDC reader, which is worse than saying nothing.
READER_ALIASES = {
    "oracle": "cdc_reader",
    "postgresql": "cdc_reader", "postgres": "cdc_reader",
    "mysql": "cdc_reader",
    "mariadb": "cdc_reader",
    "sqlserver": "cdc_reader", "mssql": "cdc_reader",
    "spanner": "cdc_reader",
    "mongodb": "cdc_reader",
    "bigquery": "batch_reader",
    "snowflake": "batch_reader",
    "databricks": "batch_reader",
    "salesforce": "api_poller",
    "workday": "api_poller",
    "netsuite": "api_poller",
    "hubspot": "api_poller",
    "shopify": "api_poller",
    "googlesheets": "spreadsheet_reader", "sheets": "spreadsheet_reader",
    "excelonline": "spreadsheet_reader",
    "kafka": "stream_reader",
    "kinesis": "stream_reader",
    "eventhub": "stream_reader",
    "pubsub": "stream_reader",
}

WRITER_ALIASES = {
    "file": "append_only_writer",
    "s3": "append_only_writer",
    "gcs": "append_only_writer",
    "adls": "append_only_writer",
    "hdfs": "append_only_writer",
    "azureblob": "append_only_writer",
    "bigquery": "append_only_writer",
    "snowflake": "append_only_writer",
    "fabric": "append_only_writer",
    "iceberg": "append_only_writer",
    "deltalake": "append_only_writer",
    "databricks": "append_only_writer",
    "redshift": "append_only_writer",
    "synapse": "append_only_writer",
    "kafka": "append_only_writer",
    "kinesis": "append_only_writer",
    "eventhub": "append_only_writer",
    "pubsub": "append_only_writer",
    "tableau": "append_only_writer",
    "slack": "append_only_writer",
}

WRITER_ADAPTERS = {
    # writers that can be repaired with bounded DML (see PLAN section 5, zone 2)
    "databasewriter": "transactional_writer",
    "spannerwriter": "transactional_writer",
    "mongodbwriter": "transactional_writer",
    "cosmosdbwriter": "transactional_writer",
    "cassandrawriter": "transactional_writer",
    "hbasewriter": "transactional_writer",

    # writers that append
    "filewriter": "append_only_writer",
    "s3writer": "append_only_writer",
    "gcswriter": "append_only_writer",
    "adlswriter": "append_only_writer",
    "azureblobwriter": "append_only_writer",
    "hdfswriter": "append_only_writer",
    "bigquerywriter": "append_only_writer",
    "snowflakewriter": "append_only_writer",
    "databrickswriter": "append_only_writer",
    "deltalakewriter": "append_only_writer",
    "icebergwriter": "append_only_writer",
    "biglakewriter": "append_only_writer",
    "fabricdatawarehousewriter": "append_only_writer",
    "redshiftwriter": "append_only_writer",
    "synapsewriter": "append_only_writer",
    "kafkawriter": "append_only_writer",
    "kinesiswriter": "append_only_writer",
    "eventhubwriter": "append_only_writer",
    "pubsubwriter": "append_only_writer",
    "jmswriter": "append_only_writer",
    "mqttwriter": "append_only_writer",
    "tableauwriter": "append_only_writer",
    "slackwriter": "append_only_writer",
    "httpwriter": "append_only_writer",
    "webhookwriter": "append_only_writer",
}


def _search(haystack, table):
    """Longest matching adapter token wins. Returns (token, behaviour)."""
    for token in sorted(table, key=len, reverse=True):
        if token in haystack:
            return token, table[token]
    return None, None


def resolve_adapter(name, role=None):
    """Work out which adapter a component name refers to.

    `role` is "source" or "target" when we know it, and it CONFINES the
    search rather than merely ordering it. A component named
    `MANIMARAN22.OracleToFabricDWH_CDC_Target` contains the word "oracle",
    but it is a target - reading that as an Oracle CDC reader would be
    confidently wrong, and worse than returning nothing.

    Full adapter names always beat product words, so a real
    `FileReaderSource` is never resolved by the loose "file" alias.
    """
    if not name:
        return None, None

    haystack = str(name).lower().replace("_", "").replace(" ", "")

    if role == "target":
        order = (WRITER_ADAPTERS, WRITER_ALIASES)
    elif role == "source":
        order = (READER_ADAPTERS, READER_ALIASES)
    else:
        order = (READER_ADAPTERS, WRITER_ADAPTERS,
                 READER_ALIASES, WRITER_ALIASES)

    for table in order:
        token, behaviour = _search(haystack, table)
        if token:
            return token, behaviour
    return None, None


def profile_for(component_name, role=None, adapter=None):
    """The behaviour profile for one component, or an explicit "unknown".

    Pass `adapter` when describe_component has given you the real adapter
    name - it beats guessing from the component name.
    """
    if not component_name and not adapter:
        return None

    token, behaviour = resolve_adapter(adapter, role)
    if not behaviour:
        token, behaviour = resolve_adapter(component_name, role)

    if not behaviour:
        return {
            "adapter_guess": "unrecognised",
            "behaviour": "unknown",
            "matched_on": None,
            **UNRECOGNISED,
        }

    return {
        "adapter_guess": adapter or token,
        "behaviour": behaviour,
        "matched_on": token,
        **BEHAVIOUR_PROFILES[behaviour],
    }


def behaviour_of(component_name, role=None, adapter=None):
    """Just the behaviour class, for code that needs to branch on it."""
    profile = profile_for(component_name, role, adapter)
    return profile["behaviour"] if profile else "unknown"
