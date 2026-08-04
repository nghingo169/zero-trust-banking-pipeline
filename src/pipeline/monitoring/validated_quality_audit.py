# Databricks notebook source
"""Record governance metrics for the normalized-and-validated Silver layer."""

import sys
import uuid
from datetime import datetime

from pyspark.sql import Row, functions as F

dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "full-source-to-validated-silver")
dbutils.widgets.text("quality_rules_path", "")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
if not RUN_ID:
    raise ValueError("run_id is required")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.audit.writer import write_audit
from data_contracts.quality_rules.registry import RULES_BY_TABLE
from data_contracts.table_catalog import DOMAINS, tables

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
VALIDATED_SCHEMA = "silver_validated"
GOVERNANCE_SCHEMA = "governance"
audit_rows = []


def qualified(schema, table_name):
    return f"{CATALOG}.{schema}.{table_name}"


def quarantine_count(domain, table_name):
    name = qualified(GOVERNANCE_SCHEMA, "silver_quarantine_record")
    if not spark.catalog.tableExists(name):
        return 0
    return spark.table(name).filter(
        (F.col("source_table_name") == F.lit(table_name))
        & (F.col("pipeline_run_id") == F.lit(RUN_ID))
    ).count()


quarantine_table = qualified(GOVERNANCE_SCHEMA, "silver_quarantine_record")
if spark.catalog.tableExists(quarantine_table):
    # This task depends on the successful validation-and-routing update. A
    # clean test run starts with no unassigned records, so only this run's
    # centralized quarantine rows are stamped with the job run identifier.
    spark.sql(
        f"""UPDATE {quarantine_table}
            SET pipeline_run_id = '{RUN_ID}'
            WHERE pipeline_run_id IS NULL"""
    )


for domain in DOMAINS:
    metrics = []
    rule_audits = []
    for table_name, business_key in tables(domain).items():
        bronze = spark.table(qualified(BRONZE_SCHEMA, table_name))
        validated = spark.table(qualified(VALIDATED_SCHEMA, table_name))
        is_scd2 = table_name in DOMAINS[domain]["scd2"]
        current = bronze.filter("__END_AT IS NULL") if is_scd2 else bronze
        duplicate_count = current.groupBy(business_key).count().filter("count > 1").count()
        interval_count = (
            bronze.filter("__END_AT IS NOT NULL AND __START_AT >= __END_AT").count()
            if is_scd2 else 0
        )
        rules = RULES_BY_TABLE.get(table_name, [])
        rule_expression = " AND ".join(
            f"coalesce(({rule['constraint']}), false)" for rule in rules
        )
        validated_rule_failures = (
            validated.filter(f"NOT ({rule_expression})").count()
            if rule_expression else 0
        )
        checked = validated.count()
        for rule_name, failed_count in (
            ("current_business_keys_unique", duplicate_count),
            ("scd2_intervals_valid", interval_count),
            ("validated_rows_pass_all_rules", validated_rule_failures),
        ):
            rule_audits.append({
                "audit_id": str(uuid.uuid4()), "run_id": RUN_ID,
                "business_date": BUSINESS_DATE, "table_name": table_name,
                "rule_name": rule_name, "records_checked": checked,
                "records_failed": failed_count, "evaluated_at": datetime.utcnow(),
            })
        metrics.append(Row(
            run_id=RUN_ID, business_date=BUSINESS_DATE, table_name=table_name,
            landing_rows=0, bronze_change_rows=bronze.count(),
            clean_current_rows=checked,
            quarantined_rows=quarantine_count(domain, table_name),
            recorded_at=datetime.utcnow(),
        ))
    write_audit(
        spark, catalog=CATALOG, domain=domain, pipeline_run_id=RUN_ID,
        pipeline_name=PIPELINE_NAME, business_date=BUSINESS_DATE,
        execution_status="SUCCEEDED",
        table_metrics=[row.asDict() for row in metrics], rule_audits=rule_audits,
    )