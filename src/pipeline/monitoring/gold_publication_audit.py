# Databricks notebook source
"""Record post-SDP publication and lineage checks for all Gold contexts."""

import sys
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


BUSINESS_DATE = widget("business_date", "2026-07-10")
RUN_ID = widget("run_id", "")
PIPELINE_NAME = widget("pipeline_name", "banking-investigation-pipeline")
RULE_PATH = widget("quality_rules_path", "")
CATALOG = widget("catalog", "workspace")
GOLD_SCHEMA = widget("gold_schema", "gold")
GOVERNANCE_SCHEMA = widget("governance_schema", "governance")

if not RUN_ID:
    raise ValueError("run_id is required")
if RULE_PATH and RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.audit.writer import write_audit

GOLD_KEYS = {
    "ai_fraud_transaction_context": "financial_event_key",
    "ai_customer_360_context": "party_key",
    "ai_aml_investigation_context": "investigation_case_key",
}


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


metrics = []
rule_audits = []
for table_name, key_column in GOLD_KEYS.items():
    table_ref = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    exists = spark.catalog.tableExists(table_ref)
    if not exists:
        rule_audits.append(audit_row(table_name, "gold_table_exists", 1, 1))
        total_rows = 0
    else:
        df = spark.table(table_ref)
        total_rows = df.count()
        duplicate_keys = (
            df.groupBy(key_column).count().filter(F.col("count") > 1).count()
            if key_column in df.columns
            else max(total_rows, 1)
        )
        lineage_failures = (
            df.filter(
                F.col("pipeline_run_id").isNull()
                | (F.col("pipeline_run_id") != F.lit(RUN_ID))
            ).count()
            if "pipeline_run_id" in df.columns
            else max(total_rows, 1)
        )
        timestamp_failures = (
            df.filter(F.col("ingested_at").isNull()).count()
            if "ingested_at" in df.columns
            else max(total_rows, 1)
        )
        rule_audits.extend(
            (
                audit_row(table_name, "gold_table_exists", 1, 0),
                audit_row(table_name, "gold_row_count_positive", 1, int(total_rows == 0)),
                audit_row(table_name, "gold_key_unique", total_rows, duplicate_keys),
                audit_row(
                    table_name,
                    "pipeline_run_id_lineage_matched",
                    total_rows,
                    lineage_failures,
                ),
                audit_row(
                    table_name,
                    "ingested_at_timestamp_valid",
                    total_rows,
                    timestamp_failures,
                ),
            )
        )

    metrics.append(
        Row(
            run_id=RUN_ID,
            business_date=BUSINESS_DATE,
            table_name=table_name,
            landing_rows=0,
            bronze_change_rows=0,
            clean_current_rows=total_rows,
            quarantined_rows=0,
            recorded_at=datetime.utcnow(),
        )
    )

write_audit(
    spark,
    catalog=CATALOG,
    domain="gold",
    pipeline_run_id=RUN_ID,
    pipeline_name=PIPELINE_NAME,
    business_date=BUSINESS_DATE,
    execution_status="RUNNING",
    table_metrics=[metric.asDict() for metric in metrics],
    rule_audits=rule_audits,
    governance_schema=GOVERNANCE_SCHEMA,
)
