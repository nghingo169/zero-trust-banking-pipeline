# Databricks notebook source
"""Record governance metrics for the normalized-and-validated Silver layer."""

import sys
import uuid
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "banking-investigation-pipeline")
dbutils.widgets.text("quality_rules_path", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("validated_schema", "silver_validated")
dbutils.widgets.text("governance_schema", "governance")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
VALIDATED_SCHEMA = dbutils.widgets.get("validated_schema")
GOVERNANCE_SCHEMA = dbutils.widgets.get("governance_schema")
if not RUN_ID:
    raise ValueError("run_id is required")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.audit.writer import write_audit
from data_contracts.quality_rules.registry import RULES_BY_TABLE
from data_contracts.table_catalog import DOMAINS, tables

audit_rows = []


def qualified(schema, table_name):
    return f"{CATALOG}.{schema}.{table_name}"


def quarantine_count(domain, table_name):
    name = qualified(GOVERNANCE_SCHEMA, "silver_quarantine_record")
    if not spark.catalog.tableExists(name):
        return 0
    return (
        spark.table(name)
        .filter(
            (F.col("source_table_name") == F.lit(table_name))
            & (F.col("pipeline_run_id") == F.lit(RUN_ID))
        )
        .count()
    )


for domain in DOMAINS:
    metrics = []
    rule_audits = []
    for table_name, business_key in tables(domain).items():
        bronze_table = qualified(BRONZE_SCHEMA, table_name)
        validated_table = qualified(VALIDATED_SCHEMA, table_name)
        bronze_exists = spark.catalog.tableExists(bronze_table)
        validated_exists = spark.catalog.tableExists(validated_table)
        bronze = spark.table(bronze_table) if bronze_exists else None
        validated = spark.table(validated_table) if validated_exists else None
        is_scd2 = table_name in DOMAINS[domain]["scd2"]
        current = (
            bronze.filter("__END_AT IS NULL") if bronze_exists and is_scd2 else bronze
        )
        duplicate_count = (
            current.groupBy(business_key).count().filter("count > 1").count()
            if current is not None
            else 0
        )
        interval_count = (
            bronze.filter("__END_AT IS NOT NULL AND __START_AT >= __END_AT").count()
            if bronze_exists and is_scd2
            else 0
        )
        rules = RULES_BY_TABLE.get(table_name, [])
        rule_expression = " AND ".join(
            f"coalesce(({rule['constraint']}), false)" for rule in rules
        )
        validated_rule_failures = (
            validated.filter(f"NOT ({rule_expression})").count()
            if validated_exists and rule_expression
            else 0
        )
        checked = validated.count() if validated_exists else 0
        for rule_name, failed_count in (
            ("bronze_table_exists", int(not bronze_exists)),
            ("validated_table_exists", int(not validated_exists)),
            ("current_business_keys_unique", duplicate_count),
            ("scd2_intervals_valid", interval_count),
            ("validated_rows_pass_all_rules", validated_rule_failures),
        ):
            rule_audits.append(
                {
                    "audit_id": str(uuid.uuid4()),
                    "run_id": RUN_ID,
                    "business_date": BUSINESS_DATE,
                    "table_name": table_name,
                    "rule_name": rule_name,
                    "records_checked": checked,
                    "records_failed": failed_count,
                    "evaluated_at": datetime.utcnow(),
                }
            )
        metrics.append(
            Row(
                run_id=RUN_ID,
                business_date=BUSINESS_DATE,
                table_name=table_name,
                landing_rows=0,
                bronze_change_rows=bronze.count() if bronze_exists else 0,
                clean_current_rows=checked,
                quarantined_rows=quarantine_count(domain, table_name),
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
        execution_status="RUNNING",
        table_metrics=[row.asDict() for row in metrics],
        rule_audits=rule_audits,
        governance_schema=GOVERNANCE_SCHEMA,
    )
