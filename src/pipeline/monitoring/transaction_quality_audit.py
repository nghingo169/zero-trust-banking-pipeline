# Databricks notebook source
"""Persist daily Transaction replay metrics and validate CDC invariants."""

import re, sys, uuid
from datetime import datetime
from pyspark.sql import Row, functions as F

dbutils.widgets.text("business_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("quality_rules_path", "")
DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
RULE_PATH = dbutils.widgets.get("quality_rules_path")
if not re.fullmatch(r"2026-07-(0[5-9]|10)", DATE) or not RUN_ID:
    raise ValueError("business_date and run_id are required")
if RULE_PATH not in sys.path: sys.path.insert(0, RULE_PATH)
from data_contracts.table_catalog import DOMAINS
from data_contracts.audit.writer import write_audit

CAT, BRONZE, SILVER, QUAR = "workspace", "dev_transaction_bronze", "dev_transaction_silver", "dev_transaction_quarantine"
LANDING_ROOT = "dbfs:/Volumes/workspace/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=customer_transaction/simulation_id=banking-20260705-20260710"
SCD2 = DOMAINS["transaction"]["scd2"]
EVENTS = DOMAINS["transaction"]["append"]
TABLES = {**SCD2, **EVENTS}
rows, failures, audit_checks = [], [], []

def full(schema, name): return f"{CAT}.{schema}.{name}"
def check(name, label, actual, passed, records_checked):
    failures.append((name, label, actual)) if not passed else None
    audit_checks.append({"audit_id": str(uuid.uuid4()), "run_id": RUN_ID, "business_date": DATE,
                         "table_name": name, "rule_name": label, "records_checked": records_checked,
                         "records_failed": 0 if passed else actual, "evaluated_at": datetime.utcnow()})

for name, key in TABLES.items():
    bronze = spark.table(full(BRONZE, name))
    current = bronze.filter("__END_AT IS NULL") if name in SCD2 else bronze
    duplicate = current.groupBy(key).count().filter("count > 1").count()
    check(name, "current_keys_unique", duplicate, duplicate == 0, current.count())
    if name in SCD2:
        bad_intervals = bronze.filter("__END_AT IS NOT NULL AND __START_AT >= __END_AT").count()
        check(name, "scd2_intervals_valid", bad_intervals, bad_intervals == 0, bronze.count())
    changes = (spark.read.option("readChangeFeed", "true").option("startingVersion", "1").table(full(BRONZE, name))
               .filter("_change_type IN ('insert', 'update_postimage')")
               .filter(F.col("business_date") == F.to_date(F.lit(DATE))).count())
    clean = spark.table(full(SILVER, name)).count()
    quarantined = spark.table(full(QUAR, name)).filter(F.col("validation_business_date") == F.to_date(F.lit(DATE))).count()
    landing_rows = spark.read.parquet(f"{LANDING_ROOT}/snapshot_type=full/business_date={DATE}/customer_transaction/{name}").count()
    rows.append(Row(run_id=RUN_ID, business_date=DATE, table_name=name, landing_rows=landing_rows,
                    bronze_change_rows=changes, clean_current_rows=clean,
                    quarantined_rows=quarantined, recorded_at=datetime.utcnow()))

write_audit(spark, catalog=CAT, domain="transaction", pipeline_run_id=RUN_ID,
    pipeline_name="dev-transaction-quality-audit", business_date=DATE,
    execution_status="FAILED" if failures else "SUCCEEDED",
    table_metrics=[row.asDict() for row in rows], rule_audits=audit_checks)
if failures:
    raise AssertionError("Transaction quality audit failed: " + ", ".join(f"{n}.{c}={a}" for n, c, a in failures))
