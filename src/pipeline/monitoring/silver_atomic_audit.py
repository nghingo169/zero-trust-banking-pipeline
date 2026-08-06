# Databricks notebook source
"""Record governance metrics and structural integrity checks for the Silver Atomic Model (`silver`).

This notebook runs immediately after `transform_silver_atomic` DLT pipeline finishes.
It evaluates all transformed Silver Atomic tables (Materialized Views / Tables),
verifying record counts, primary key uniqueness, and row-level lineage matching the official job run_id.
"""

import sys
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

# Databricks Widgets for parameters passed from Workflow Job Task
dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "banking-investigation-pipeline")
dbutils.widgets.text("quality_rules_path", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("governance_schema", "governance")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOVERNANCE_SCHEMA = dbutils.widgets.get("governance_schema")

if not RUN_ID:
    raise ValueError("run_id is required")
if RULE_PATH and RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.audit.writer import write_audit
from data_contracts.table_catalog import DOMAINS, tables

# Primary Keys / Business Keys Mapping for Silver Atomic Model
SILVER_ATOMIC_KEYS = {
    # Party / Customer Domain
    "party": "party_key",
    "party_identifier": "party_identifier_key",
    "party_identity_resolution": "identity_resolution_key",
    "party_profile_version": "party_profile_version_key",
    "party_kyc_assessment": "kyc_assessment_key",
    "party_employment": "employment_key",
    "party_service_request": "service_request_key",
    # Account & Card System Domain
    "account": "account_key",
    "party_account_role": "party_account_role_key",
    "payment_card": "payment_card_key",
    "payment_card_limit_history": "card_limit_history_key",
    "account_balance_snapshot": "account_balance_snapshot_key",
    "transaction_channel": "channel_key",
    "merchant": "merchant_key",
    "merchant_location": "merchant_location_key",
    # Financial Event Domain
    "financial_event": "financial_event_key",
    "account_posting": "financial_event_key",
    "card_payment": "financial_event_key",
    "atm_activity": "financial_event_key",
    "gateway_payment": "financial_event_key",
    "financial_event_status_history": "financial_event_status_history_key",
    # FinCrime & AML Domain
    "financial_event_risk_score": "financial_event_risk_score_key",
    "fraud_alert": "fraud_alert_key",
    "financial_event_fraud_alert": "financial_event_fraud_alert_key",
    "transaction_monitoring_alert": "monitoring_alert_key",
    "monitoring_alert_financial_event": "monitoring_alert_financial_event_key",
    "investigation_case": "investigation_case_key",
    "investigation_case_financial_event": "investigation_case_financial_event_key",
    "investigation_case_fraud_alert": "investigation_case_fraud_alert_key",
    "investigation_case_monitoring_alert": "investigation_case_monitoring_alert_key",
    "investigation_note": "investigation_note_key",
    "aml_case": "aml_case_key",
    "watchlist_entry": "watchlist_entry_key",
    "sanctions_screening": "sanctions_screening_key",
    "investigation_case_sanctions_screening": "investigation_case_sanctions_screening_key",
    "suspicious_activity_report": "suspicious_activity_report_key",
    "card_fraud_flag": "card_fraud_flag_key",
    "financial_event_card_fraud_flag": "financial_event_card_fraud_flag_key",
    "chargeback": "chargeback_key",
    "call_center_contact": "call_center_contact_key",
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


# Evaluate quality & metrics for each domain in Silver Atomic Layer
for domain in DOMAINS:
    metrics = []
    rule_audits = []

    for table_name in tables(domain).keys():
        silver_table = qualified(SILVER_SCHEMA, table_name)

        # 1. Check Table Existence in Catalog
        if not spark.catalog.tableExists(silver_table):
            rule_audits.append(
                audit_row(table_name, "silver_atomic_table_exists", 1, 1)
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

        silver_df = spark.table(silver_table)
        total_rows = silver_df.count()

        # 2. Primary Key Uniqueness Check
        primary_key = SILVER_ATOMIC_KEYS.get(table_name)
        duplicate_keys = 0
        if primary_key and primary_key in silver_df.columns:
            duplicate_keys = (
                silver_df.groupBy(primary_key)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

        # 3. Pipeline Run ID Lineage Completeness & Correctness Check
        missing_or_mismatched_run_id = (
            silver_df.filter(
                F.col("pipeline_run_id").isNull()
                | (F.col("pipeline_run_id") == F.lit(""))
                | (F.col("pipeline_run_id") != F.lit(RUN_ID))
            ).count()
            if "pipeline_run_id" in silver_df.columns
            else max(total_rows, 1)
        )

        # 4. Ingested Timestamp Validity Check
        missing_ingested_at = (
            silver_df.filter(F.col("ingested_at").isNull()).count()
            if "ingested_at" in silver_df.columns
            else max(total_rows, 1)
        )

        # Append Rule Evaluations
        rule_audits.extend(
            [
                audit_row(table_name, "silver_atomic_table_exists", 1, 0),
                audit_row(
                    table_name,
                    "atomic_primary_key_unique",
                    total_rows,
                    duplicate_keys,
                ),
                audit_row(
                    table_name,
                    "pipeline_run_id_lineage_matched",
                    total_rows,
                    missing_or_mismatched_run_id,
                ),
                audit_row(
                    table_name,
                    "ingested_at_timestamp_valid",
                    total_rows,
                    missing_ingested_at,
                ),
            ]
        )

        # Append Domain Metrics
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

    # Publish audit evidence to Governance schema
    write_audit(
        spark,
        catalog=CATALOG,
        domain=domain,
        pipeline_run_id=RUN_ID,
        pipeline_name=PIPELINE_NAME,
        business_date=BUSINESS_DATE,
        execution_status="RUNNING",
        table_metrics=[metric.asDict() for metric in metrics],
        rule_audits=rule_audits,
        governance_schema=GOVERNANCE_SCHEMA,
    )
