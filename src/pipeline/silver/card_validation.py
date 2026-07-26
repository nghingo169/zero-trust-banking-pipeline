"""Card Bronze-to-Silver validation and quarantine.

The Bronze pipeline owns CDC. This pipeline validates its outputs, preserves
valid SCD2 and immutable-event history in Silver, and appends invalid CDF records to per-table
quarantine tables with the source business date and failed rule names.
"""

from __future__ import annotations

import sys

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The bundle exposes its synchronized src/ directory as a workspace file path.
# Adding that directory to sys.path lets this pipeline use the one tested,
# executable registry rather than maintaining a copied rule dictionary.
QUALITY_RULES_PATH = spark.conf.get("pipeline.quality_rules_path")
if QUALITY_RULES_PATH not in sys.path:
    sys.path.insert(0, QUALITY_RULES_PATH)

from data_contracts.quality_rules.registry import get_rules
from data_contracts.table_catalog import DOMAINS

CATALOG = spark.conf.get("pipeline.catalog")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema")
QUARANTINE_SCHEMA = spark.conf.get("pipeline.quarantine_schema")

TABLE_CONFIGS = {
    name: {"key": key, "is_scd2": True}
    for name, key in DOMAINS["card"]["scd2"].items()
} | {
    name: {"key": key, "is_scd2": False}
    for name, key in DOMAINS["card"]["append"].items()
}


# ==================
# Returns the configured Bronze table name.
# ==================
def _bronze_table(table_name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


# ==================
# Returns the matching isolated quarantine table name.
# ==================
def _quarantine_table(table_name: str) -> str:
    return f"{CATALOG}.{QUARANTINE_SCHEMA}.{table_name}"


# ==================
# Reads the complete SCD2 history or immutable event history for Silver.
# ==================
def _silver_rows(table_name: str) -> DataFrame:
    return spark.read.table(_bronze_table(table_name))


def _current_rows(table_name: str) -> DataFrame:
    df = _silver_rows(table_name)
    if TABLE_CONFIGS[table_name]["is_scd2"]:
        df = df.filter("__END_AT IS NULL")
    return df


# ==================
# Adds failed-rule names and quarantine metadata to each row.
# ==================
def _with_validation_metadata(df: DataFrame, table_name: str) -> DataFrame:
    rules = get_rules(table_name)
    failed_rule_names = F.concat_ws(
        ",",
        *[
            # SQL predicates can evaluate to NULL. A quality rule passes only
            # when it evaluates to true, so NULL must be quarantined too.
            F.when(~F.coalesce(F.expr(constraint), F.lit(False)), F.lit(name))
            for name, constraint in rules.items()
        ],
    )
    return (
        df.withColumn("validation_business_date", F.col("business_date"))
        .withColumn("source_table", F.lit(table_name))
        .withColumn("failed_rules", failed_rule_names)
        .withColumn("is_quarantined", F.col("failed_rules") != "")
    )


# ==================
# Prepares every Bronze version for Lakeflow expectations.
# ==================
def _validation_rows(table_name: str) -> DataFrame:
    return _with_validation_metadata(_silver_rows(table_name), table_name)


# ==================
# Reads each new Bronze change once for quarantine history.
# ==================
def _quarantine_change_rows(table_name: str) -> DataFrame:
    """Return each newly written Bronze row once via Delta CDF."""

    df = (
        spark.readStream.option("readChangeFeed", "true")
        .table(_bronze_table(table_name))
        .filter("_change_type IN ('insert', 'update_postimage')")
    )
    return (
        _with_validation_metadata(
            df.drop("_change_type", "_commit_version", "_commit_timestamp"),
            table_name,
        )
        .filter("is_quarantined")
    )


# ==================
# Creates validation, clean Silver, and quarantine datasets for one table.
# ==================
def _register_table(table_name: str) -> None:
    rules = get_rules(table_name)
    validation_name = f"validation_{table_name}"

    # ==================
    # Applies quality rules in a temporary validation table.
    # ==================
    @dp.table(name=validation_name, temporary=True)
    @dp.expect_all(rules)
    def validation_table() -> DataFrame:
        return _validation_rows(table_name)

    # ==================
    # Publishes only rows that pass every quality rule.
    # ==================
    @dp.table(name=table_name)
    def clean_silver_table() -> DataFrame:
        return (
            spark.read.table(validation_name)
            .filter("NOT is_quarantined")
            .drop("is_quarantined", "failed_rules", "source_table")
        )

    # ==================
    # Saves failing rows with their date and failed rule names.
    # ==================
    @dp.table(name=_quarantine_table(table_name))
    def quarantine_history_table() -> DataFrame:
        return _quarantine_change_rows(table_name)


# ==================
# Finds duplicate keys for monitoring without changing Bronze data.
# ==================
def _duplicate_key_rows(table_name: str) -> DataFrame:
    key = TABLE_CONFIGS[table_name]["key"]
    df = _current_rows(table_name)
    return (
        df.groupBy("business_date", key)
        .count()
        .filter("count > 1")
        .select(
            F.lit(table_name).alias("source_table"),
            F.col("business_date"),
            F.lit(key).alias("key_column"),
            F.col(key).cast("string").alias("key_value"),
            F.col("count").alias("duplicate_count"),
        )
    )


@dp.table(name="card_transaction_current_status")
def card_transaction_current_status() -> DataFrame:
    """Current card-transaction state derived from immutable status events."""

    events = spark.read.table(_bronze_table("card_transaction_status_event"))
    ordering = F.struct(
        F.col("status_timestamp"),
        F.col("sequence_number"),
        F.col("status_event_id"),
    )
    latest = events.groupBy("card_txn_id").agg(F.max(ordering).alias("latest_sequence"))
    return (
        events.join(latest, "card_txn_id")
        .where(ordering == F.col("latest_sequence"))
        .select(
            "card_txn_id", F.col("status").alias("current_status"),
            "status_timestamp", "sequence_number", "status_event_id",
        )
    )


# ==================
# Publishes duplicate-key findings for all Card tables.
# ==================
@dp.table(name="quality_duplicate_key_monitor")
def quality_duplicate_key_monitor() -> DataFrame:
    duplicate_sets = [_duplicate_key_rows(name) for name in TABLE_CONFIGS]
    result = duplicate_sets[0]
    for duplicate_set in duplicate_sets[1:]:
        result = result.unionByName(duplicate_set)
    return result


for _table_name in TABLE_CONFIGS:
    _register_table(_table_name)
