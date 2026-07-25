"""Two-day Card POC: Bronze-to-Silver validation and quarantine.

The Bronze pipeline owns CDC. This pipeline validates its outputs, publishes
current valid Silver tables, and appends invalid CDF records to per-table
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

from quality_rules import get_rules

CATALOG = spark.conf.get("pipeline.catalog")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema")
QUARANTINE_SCHEMA = spark.conf.get("pipeline.quarantine_schema")

TABLE_CONFIGS = {
    "card": {"key": "card_id", "is_scd2": True},
    "card_transaction": {"key": "card_txn_id", "is_scd2": False},
    "card_fraud_flag": {"key": "flag_id", "is_scd2": True},
    "card_limit_history": {"key": "history_id", "is_scd2": False},
    "card_transaction_status_event": {"key": "status_event_id", "is_scd2": False},
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
# Reads current entity rows or all immutable Bronze rows for validation.
# ==================
def _current_rows(table_name: str) -> DataFrame:
    df = spark.read.table(_bronze_table(table_name))
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
            F.when(~F.expr(constraint), F.lit(name))
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
# Prepares the current Bronze rows for Lakeflow expectations.
# ==================
def _current_validation_rows(table_name: str) -> DataFrame:
    return _with_validation_metadata(_current_rows(table_name), table_name)


# ==================
# Reads each new Bronze change once for quarantine history.
# ==================
def _quarantine_change_rows(table_name: str) -> DataFrame:
    """Return each newly written valid/current Bronze row once via Delta CDF."""

    df = (
        spark.readStream.option("readChangeFeed", "true")
        .table(_bronze_table(table_name))
        .filter("_change_type IN ('insert', 'update_postimage')")
    )
    if TABLE_CONFIGS[table_name]["is_scd2"]:
        df = df.filter("__END_AT IS NULL")
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
        return _current_validation_rows(table_name)

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
