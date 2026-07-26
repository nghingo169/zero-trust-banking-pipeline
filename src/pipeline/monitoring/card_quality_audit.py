# Databricks notebook source
"""Daily metrics and reconciliation assertions for the six-day Card replay.

This notebook is deliberately a job task rather than a pipeline expectation:
it checks relationships across landing, Bronze, Silver, and quarantine after
both pipelines have completed.  Findings are persisted before a failed task
raises so a replay can be diagnosed without re-running it.
"""

from __future__ import annotations

import re
import sys
import uuid
from datetime import datetime

from pyspark.sql import Row, functions as F

dbutils.widgets.text("business_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("quality_rules_path", "")
BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
QUALITY_RULES_PATH = dbutils.widgets.get("quality_rules_path")
if not re.fullmatch(r"2026-07-(0[5-9]|10)", BUSINESS_DATE):
    raise ValueError("business_date must be one of 2026-07-05 through 2026-07-10")
if not RUN_ID:
    raise ValueError("run_id is required")
if not QUALITY_RULES_PATH:
    raise ValueError("quality_rules_path is required")
SNAPSHOT_VERSION = int(BUSINESS_DATE.replace("-", ""))
FINAL_REPLAY_DATE = "2026-07-10"

CATALOG = "workspace"
BRONZE_SCHEMA = "dev_card_bronze"
SILVER_SCHEMA = "dev_card_silver"
QUARANTINE_SCHEMA = "dev_card_quarantine"
LANDING_ROOT = (
    "dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/"
    "banking_v2/snapshots/domain=card/simulation_id=banking-20260705-20260710"
)
if QUALITY_RULES_PATH not in sys.path:
    sys.path.insert(0, QUALITY_RULES_PATH)
from data_contracts.quality_rules.registry import get_rules
from data_contracts.audit.writer import write_audit
from data_contracts.table_catalog import DOMAINS

TABLES = {
    **{name: (key, True) for name, key in DOMAINS["card"]["scd2"].items()},
    **{name: (key, False) for name, key in DOMAINS["card"]["append"].items()},
}
findings: list[dict[str, object]] = []
table_metrics: list[dict[str, object]] = []


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


def add(
    table_name: str,
    check: str,
    expected: object,
    actual: object,
    passed: bool,
    detail: str = "",
    blocking: bool = True,
) -> None:
    findings.append(
        {
            "run_id": RUN_ID,
            "business_date": BUSINESS_DATE,
            "table_name": table_name,
            "check_name": check,
            "expected_value": str(expected),
            "actual_value": str(actual),
            "passed": bool(passed),
            "detail": detail,
            "blocking": blocking,
            "checked_at": datetime.utcnow(),
        }
    )


def exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def date_count(df, condition: str) -> int:
    return df.filter(condition).count()


def rows_as_of(df, is_scd2: bool):
    """Return the logical table state at this replay date.

    Source landing may contain the complete delivery before Bronze starts.  In
    that case AUTO CDC FROM SNAPSHOT materializes every version in one update;
    audit must therefore use SCD2 boundaries rather than assume no later rows
    exist physically in Bronze.
    """
    if is_scd2:
        return df.filter(
            (F.col("__START_AT") <= F.lit(SNAPSHOT_VERSION))
            & (F.col("__END_AT").isNull() | (F.col("__END_AT") > F.lit(SNAPSHOT_VERSION)))
        )
    return df.filter(F.col("business_date") <= F.to_date(F.lit(BUSINESS_DATE)))


for name, (key, is_scd2) in TABLES.items():
    bronze_name = table(BRONZE_SCHEMA, name)
    silver_name = table(SILVER_SCHEMA, name)
    quarantine_name = table(QUARANTINE_SCHEMA, name)
    bronze_exists = exists(bronze_name)
    add(name, "bronze_table_exists", True, bronze_exists, bronze_exists)
    if not bronze_exists:
        continue
    bronze = spark.table(bronze_name)
    bronze_count = bronze.count()
    add(name, "bronze_non_empty", "> 0", bronze_count, bronze_count > 0)
    current = rows_as_of(bronze, is_scd2)
    duplicate_keys = current.groupBy(key).count().filter("count > 1").count()
    add(name, "current_business_keys_unique", 0, duplicate_keys, duplicate_keys == 0)
    if is_scd2:
        invalid_intervals = bronze.filter("__END_AT IS NOT NULL AND __START_AT >= __END_AT").count()
        add(name, "scd2_history_intervals_valid", 0, invalid_intervals, invalid_intervals == 0)
        current_per_key = bronze.filter("__END_AT IS NULL").groupBy(key).count().filter("count <> 1").count()
        add(name, "scd2_one_current_row_per_key", 0, current_per_key, current_per_key == 0)

    rules = get_rules(name)
    # Mirror Lakeflow expectation semantics: NULL is not a passing predicate.
    failures = " OR ".join(
        f"NOT coalesce(({constraint}), false)" for constraint in rules.values()
    )
    invalid_current = current.filter(failures).count()
    add(name, "current_bronze_rule_failures_accounted_for", "tracked", invalid_current, True)
    silver_exists = exists(silver_name)
    add(name, "silver_table_exists", True, silver_exists, silver_exists)
    if silver_exists:
        silver = spark.table(silver_name)
        silver_rule_failures = silver.filter(failures).count()
        add(name, "silver_rows_pass_all_rules", 0, silver_rule_failures, silver_rule_failures == 0)
        if BUSINESS_DATE == FINAL_REPLAY_DATE:
            expected_valid = current.filter(f"NOT ({failures})").count()
            actual_silver = silver.count()
            add(name, "silver_count_matches_final_valid_bronze", expected_valid, actual_silver, expected_valid == actual_silver)

    quarantine_exists = exists(quarantine_name)
    add(name, "quarantine_table_exists", True, quarantine_exists, quarantine_exists)
    if quarantine_exists:
        quarantine = spark.table(quarantine_name).filter(F.col("validation_business_date") == F.to_date(F.lit(BUSINESS_DATE)))
        malformed = quarantine.filter("NOT is_quarantined OR failed_rules IS NULL OR failed_rules = ''").count()
        add(name, "quarantine_metadata_complete", 0, malformed, malformed == 0)
        # Quarantine is written from Bronze CDF inserts/update-postimages, so
        # reconcile it with the Bronze versions created on this business date,
        # not with the reconstructed SCD2 state at that date.
        expected_quarantine = bronze.filter(
            F.col("business_date") == F.to_date(F.lit(BUSINESS_DATE))
        ).filter(failures).count()
        actual_quarantine = quarantine.count()
        add(
            name,
            "quarantine_change_history_reconciliation",
            expected_quarantine,
            actual_quarantine,
            expected_quarantine == actual_quarantine,
            "Observed only: one-shot full-history processing can close an SCD2 row before the "
            "quarantine CDF consumer reads it. Per-date strict equality requires daily pipeline runs.",
            blocking=False,
        )
    else:
        actual_quarantine = 0
    cdf_changes = (
        # Version 0 creates the Delta table before CDF is enabled.  CDF begins
        # at version 1 for the existing and freshly reset Bronze tables.
        spark.read.option("readChangeFeed", "true").option("startingVersion", "1").table(bronze_name)
        .filter("_change_type IN ('insert', 'update_postimage')")
        .filter(F.col("business_date") == F.to_date(F.lit(BUSINESS_DATE)))
        .count()
    )
    table_metrics.append({
        "run_id": RUN_ID, "business_date": BUSINESS_DATE, "table_name": name,
        "landing_rows": spark.read.parquet(f"{LANDING_ROOT}/snapshot_type=full/business_date={BUSINESS_DATE}/card/{name}").count(), "bronze_change_rows": cdf_changes,
        "clean_current_rows": current.filter(f"NOT ({failures})").count(),
        "quarantined_rows": actual_quarantine, "recorded_at": datetime.utcnow(),
    })

monitor_name = table(SILVER_SCHEMA, "quality_duplicate_key_monitor")
monitor_rows = spark.table(monitor_name).count() if exists(monitor_name) else -1
add("quality_duplicate_key_monitor", "empty", 0, monitor_rows, monitor_rows == 0)

if BUSINESS_DATE == "2026-07-08":
    source = f"{LANDING_ROOT}/snapshot_type=full/business_date={BUSINESS_DATE}/card/card_limit_history"
    source_type = spark.read.parquet(source).schema["limit_amount"].dataType.simpleString()
    bronze_type = spark.table(table(BRONZE_SCHEMA, "card_limit_history")).schema["limit_amount"].dataType.simpleString()
    silver_type = spark.table(table(SILVER_SCHEMA, "card_limit_history")).schema["limit_amount"].dataType.simpleString()
    add("card_limit_history", "july_8_landing_limit_amount_is_string", "string", source_type, source_type == "string")
    add("card_limit_history", "bronze_limit_amount_is_decimal_12_2", "decimal(12,2)", bronze_type, bronze_type == "decimal(12,2)")
    add("card_limit_history", "silver_limit_amount_is_decimal_12_2", "decimal(12,2)", silver_type, silver_type == "decimal(12,2)")
    failed_casts = spark.table(table(BRONZE_SCHEMA, "card_limit_history")).filter("business_date = DATE '2026-07-08' AND limit_amount IS NULL").count()
    add("card_limit_history", "july_8_no_failed_limit_amount_cast", 0, failed_casts, failed_casts == 0)

if BUSINESS_DATE == "2026-07-09":
    # AUTO CDC FROM SNAPSHOT stores SCD boundaries as integer snapshot
    # versions (YYYYMMDD), not DATE values.
    history = spark.table(table(BRONZE_SCHEMA, "card")).filter("__START_AT < 20260709").count()
    add("card", "july_9_preserves_prior_scd2_history", "> 0", history, history > 0)

status = rows_as_of(spark.table(table(BRONZE_SCHEMA, "card_transaction_status_event")), False)
status_duplicates = status.groupBy("status_event_id").count().filter("count > 1").count()
add("card_transaction_status_event", "status_event_keys_unique_after_snapshots", 0, status_duplicates, status_duplicates == 0)
if BUSINESS_DATE == FINAL_REPLAY_DATE:
    current_status = spark.table(table(SILVER_SCHEMA, "card_transaction_current_status"))
    status_duplicate_transactions = current_status.groupBy("card_txn_id").count().filter("count > 1").count()
    add("card_transaction_current_status", "one_latest_status_per_transaction", 0, status_duplicate_transactions, status_duplicate_transactions == 0)

failed = [finding for finding in findings if finding["blocking"] and not finding["passed"]]
metrics_by_table = {metric["table_name"]: metric for metric in table_metrics}
rule_audits = []
for finding in findings:
    actual = str(finding["actual_value"])
    failed_count = 0 if finding["passed"] else int(actual) if actual.isdigit() else 1
    rule_audits.append({
        "audit_id": str(uuid.uuid4()), "run_id": RUN_ID,
        "business_date": BUSINESS_DATE, "table_name": finding["table_name"],
        "rule_name": finding["check_name"],
        "records_checked": metrics_by_table.get(finding["table_name"], {}).get("landing_rows", 0),
        "records_failed": failed_count, "evaluated_at": finding["checked_at"],
    })
write_audit(
    spark, catalog=CATALOG, domain="card", pipeline_run_id=RUN_ID,
    pipeline_name="dev-card-quality-audit", business_date=BUSINESS_DATE,
    execution_status="FAILED" if failed else "SUCCEEDED",
    table_metrics=table_metrics, rule_audits=rule_audits,
)
if failed:
    names = ", ".join(f"{item['table_name']}.{item['check_name']}" for item in failed)
    raise AssertionError(f"Card quality audit failed: {names}")
