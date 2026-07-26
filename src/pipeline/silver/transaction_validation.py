"""Transaction clean Silver, quarantine, and documented replay/status monitors."""

import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RULE_PATH = spark.conf.get("pipeline.quality_rules_path")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)
from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS

CAT = spark.conf.get("pipeline.catalog")
BRONZE = spark.conf.get("pipeline.bronze_schema")
QUAR = spark.conf.get("pipeline.quarantine_schema")
TABLES = {**DOMAINS["transaction"]["scd2"], **DOMAINS["transaction"]["append"]}
SCD2 = set(DOMAINS["transaction"]["scd2"])
# Only these source tables have injected-error rules in the development dataset.
# catalog. The remaining reference tables are intentionally pass-through.
QUALITY_RULE_TABLES = {
    "account_transaction",
    "account_transaction_status_event",
    "atm_transaction_status_event",
    "payment_gateway_status_event",
    "merchant_store",
    "log_atm",
    "payment_gateway_log",
    "balance_snapshot",
}


def b(n):
    return f"{CAT}.{BRONZE}.{n}"


def q(n):
    return f"{CAT}.{QUAR}.{n}"


def current(n):
    x = spark.read.table(b(n))
    return x.filter("__END_AT IS NULL") if n in SCD2 else x


def with_validation_metadata(x, n, rules):
    failed = (
        F.concat_ws(
            ",",
            *[
                F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(k))
                for k, c in rules.items()
            ],
        )
        if rules
        else F.lit("")
    )
    return (
        x.withColumn("validation_business_date", F.col("business_date"))
        .withColumn("source_table", F.lit(n))
        .withColumn("failed_rules", failed)
        .withColumn("is_quarantined", failed != "")
    )


def valid(n, rules):
    return with_validation_metadata(current(n), n, rules)


def quarantine_changes(n, rules):
    changes = (
        spark.readStream.option("readChangeFeed", "true")
        .table(b(n))
        .filter("_change_type IN ('insert', 'update_postimage')")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    if n in SCD2:
        changes = changes.filter("__END_AT IS NULL")
    return with_validation_metadata(changes, n, rules).filter("is_quarantined")


def register(n):
    v = f"validation_{n}"
    rules = get_rules(n) if n in QUALITY_RULE_TABLES else {}

    if rules:

        @dp.table(name=v, temporary=True)
        @dp.expect_all(rules)
        def validation(n=n, rules=rules):
            return valid(n, rules)

    else:

        @dp.table(name=v, temporary=True)
        def validation(n=n, rules=rules):
            return valid(n, rules)

    @dp.table(name=n)
    def clean(v=v):
        return (
            spark.read.table(v)
            .filter("NOT is_quarantined")
            .drop("is_quarantined", "failed_rules", "source_table")
        )

    @dp.table(name=q(n))
    def quarantine(n=n, rules=rules):
        return quarantine_changes(n, rules)


for n in TABLES:
    register(n)


@dp.table(name="account_transaction_current_status")
def account_transaction_current_status():
    """Current transaction state derived from immutable status-event history."""

    events = spark.read.table(b("account_transaction_status_event"))
    ordering = F.struct(
        F.col("status_timestamp"),
        F.col("sequence_number"),
        F.col("status_event_id"),
    )
    latest_sequence = events.groupBy("account_txn_id").agg(
        F.max(ordering).alias("latest_sequence")
    )
    return (
        events.join(latest_sequence, "account_txn_id")
        .where(ordering == F.col("latest_sequence"))
        .select(
            "account_txn_id",
            F.col("status").alias("current_status"),
            "status_timestamp",
            "sequence_number",
            "status_event_id",
        )
    )


@dp.table(name="transaction_replay_duplicate_monitor")
def replay_duplicates():
    return (
        current("account_transaction_status_event")
        .groupBy("status_event_id")
        .count()
        .filter("count > 1")
    )
