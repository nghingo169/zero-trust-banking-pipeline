# Databricks notebook source
"""Record post-ingestion Bronze quality evidence in the shared governance schema.

This notebook observes Bronze completeness and structural health.  It never
routes or quarantines records; the dependent validation pipeline performs that
row-level decision.
"""

import sys
import uuid
from datetime import datetime

from pyspark.sql import Row, functions as F


dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "full-source-to-validated-silver")
dbutils.widgets.text("quality_rules_path", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")

if not RUN_ID:
    raise ValueError("run_id is required")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.audit.writer import write_audit
from data_contracts.table_catalog import DOMAINS, tables


EVENT_KEYS = {
    "account_transaction_status_event": ["status_event_id", "account_txn_id"],
    "atm_transaction_status_event": ["status_event_id", "account_txn_id"],
    "payment_gateway_status_event": [
        "status_event_id",
        "gateway_txn_id",
        "event_parent_ref",
    ],
    "card_transaction_status_event": ["status_event_id", "card_txn_id"],
}


def qualified(schema: str, table_name: str) -> str:
    return f"{CATALOG}.{schema}.{table_name}"


def audit_row(table_name: str, rule_name: str, checked: int, failed: int) -> dict:
    return {
        "audit_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "business_date": BUSINESS_DATE,
        "table_name": table_name,
        "rule_name": rule_name,
        "records_checked": checked,
        "records_failed": failed,
        "evaluated_at": datetime.utcnow(),
    }


for domain in DOMAINS:
    metrics = []
    rule_audits = []

    for table_name, business_key in tables(domain).items():
        bronze_table = qualified(BRONZE_SCHEMA, table_name)

        if not spark.catalog.tableExists(bronze_table):
            rule_audits.append(
                audit_row(table_name, "bronze_table_exists", 1, 1)
            )
            metrics.append(
                Row(
                    run_id=RUN_ID,
                    business_date=BUSINESS_DATE,
                    table_name=table_name,
                    landing_rows=0,
                    bronze_change_rows=0,
                    clean_current_rows=0,
                    quarantined_rows=0,
                    recorded_at=datetime.utcnow(),
                )
            )
            continue

        bronze = spark.table(bronze_table)
        is_scd2 = table_name in DOMAINS[domain]["scd2"]
        current = bronze.filter(F.col("__END_AT").isNull()) if is_scd2 else bronze

        total_rows = bronze.count()
        current_rows = current.count()
        snapshot_rows = current.filter(
            F.col("business_date") == F.to_date(F.lit(BUSINESS_DATE))
        ).count()

        key_columns = EVENT_KEYS.get(table_name, [business_key])
        duplicate_keys = (
            current.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        rescued_rows = (
            current.filter(F.col("_rescued_data").isNotNull()).count()
            if "_rescued_data" in current.columns
            else 0
        )
        missing_file_lineage = (
            current.filter(
                F.col("source_file_name").isNull()
                | F.col("source_file_modified_at").isNull()
            ).count()
            if {
                "source_file_name",
                "source_file_modified_at",
            }.issubset(current.columns)
            else current_rows
        )
        invalid_intervals = (
            bronze.filter(F.col("__END_AT") <= F.col("__START_AT")).count()
            if is_scd2
            else 0
        )

        rule_audits.extend(
            [
                audit_row(table_name, "bronze_table_exists", 1, 0),
                audit_row(
                    table_name,
                    "current_snapshot_business_date_present",
                    current_rows,
                    int(current_rows > 0 and snapshot_rows == 0),
                ),
                audit_row(
                    table_name,
                    "current_business_keys_unique",
                    current_rows,
                    duplicate_keys,
                ),
                audit_row(
                    table_name,
                    "source_file_lineage_complete",
                    current_rows,
                    missing_file_lineage,
                ),
                audit_row(
                    table_name,
                    "rescued_data_empty",
                    current_rows,
                    rescued_rows,
                ),
            ]
        )

        if is_scd2:
            rule_audits.append(
                audit_row(
                    table_name,
                    "scd2_intervals_valid",
                    total_rows,
                    invalid_intervals,
                )
            )

        metrics.append(
            Row(
                run_id=RUN_ID,
                business_date=BUSINESS_DATE,
                table_name=table_name,
                landing_rows=0,
                bronze_change_rows=total_rows,
                clean_current_rows=current_rows,
                quarantined_rows=0,
                recorded_at=datetime.utcnow(),
            )
        )

    write_audit(
        spark,
        catalog=CATALOG,
        domain=domain,
        pipeline_run_id=RUN_ID,
        pipeline_name=PIPELINE_NAME,
        business_date=BUSINESS_DATE,
        execution_status="SUCCEEDED",
        table_metrics=[metric.asDict() for metric in metrics],
        rule_audits=rule_audits,
    )