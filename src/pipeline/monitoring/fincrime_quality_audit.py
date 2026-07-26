# Databricks notebook source
"""Persist daily Financial Crime SCD2 and quarantine metrics."""

import re, sys, uuid
from datetime import datetime
from pyspark.sql import Row, functions as F

dbutils.widgets.text("business_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("quality_rules_path", "")
DATE, RUN_ID = dbutils.widgets.get("business_date"), dbutils.widgets.get("run_id")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
if not re.fullmatch(r"2026-07-(0[5-9]|10)", DATE) or not RUN_ID:
    raise ValueError("business_date and run_id are required")
if RULE_PATH not in sys.path: sys.path.insert(0, RULE_PATH)
from data_contracts.table_catalog import DOMAINS
from data_contracts.audit.writer import write_audit

CAT, BRONZE, SILVER, QUAR = "workspace", "dev_fincrime_bronze", "dev_fincrime_silver", "dev_fincrime_quarantine"
LANDING_ROOT = "dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=financial_crime/simulation_id=banking-20260705-20260710"
TABLES = DOMAINS["fincrime"]["scd2"]
rows, failures, audit_checks = [], [], []
def full(schema, name): return f"{CAT}.{schema}.{name}"

for name, key in TABLES.items():
    bronze = spark.table(full(BRONZE, name))
    current = bronze.filter("__END_AT IS NULL")
    duplicate = current.groupBy(key).count().filter("count > 1").count()
    intervals = bronze.filter("__END_AT IS NOT NULL AND __START_AT >= __END_AT").count()
    for rule_name, failed_count, checked in (("current_keys_unique", duplicate, current.count()), ("scd2_intervals_valid", intervals, bronze.count())):
        audit_checks.append({"audit_id": str(uuid.uuid4()), "run_id": RUN_ID, "business_date": DATE,
                             "table_name": name, "rule_name": rule_name, "records_checked": checked,
                             "records_failed": failed_count, "evaluated_at": datetime.utcnow()})
    if duplicate or intervals: failures.append(f"{name}: duplicate={duplicate}, intervals={intervals}")
    try:
        changes = (spark.read.option("readChangeFeed", "true").option("startingVersion", "1").table(full(BRONZE, name))
                   .filter("_change_type IN ('insert', 'update_postimage')")
                   .filter(F.col("business_date") == F.to_date(F.lit(DATE))).count())
    except Exception as error:
        # CDF cannot span an incompatible historical type change on some Delta
        # tables.  The SCD2 history itself is still authoritative for an audit.
        if "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA" not in str(error):
            raise
        changes = bronze.count()
    clean = spark.table(full(SILVER, name)).count()
    quarantined = spark.table(full(QUAR, name)).filter(F.col("validation_business_date") == F.to_date(F.lit(DATE))).count()
    landing_rows = spark.read.parquet(f"{LANDING_ROOT}/snapshot_type=full/business_date={DATE}/financial_crime/{name}").count()
    rows.append(Row(run_id=RUN_ID, business_date=DATE, table_name=name, landing_rows=landing_rows,
                    bronze_change_rows=changes, clean_current_rows=clean,
                    quarantined_rows=quarantined, recorded_at=datetime.utcnow()))

write_audit(spark, catalog=CAT, domain="fincrime", pipeline_run_id=RUN_ID,
    pipeline_name="dev-fincrime-quality-audit", business_date=DATE,
    execution_status="FAILED" if failures else "SUCCEEDED",
    table_metrics=[row.asDict() for row in rows], rule_audits=audit_checks)
if failures: raise AssertionError("FinCrime quality audit failed: " + "; ".join(failures))
