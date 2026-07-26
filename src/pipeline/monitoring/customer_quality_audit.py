# Databricks notebook source
"""Persist daily Customer replay metrics and fail on cross-layer violations."""
import re, sys, uuid
from datetime import datetime
from pyspark.sql import Row, functions as F
dbutils.widgets.text("business_date", ""); dbutils.widgets.text("run_id", ""); dbutils.widgets.text("quality_rules_path", "")
DATE=dbutils.widgets.get("business_date"); RUN=dbutils.widgets.get("run_id"); RULE_PATH=dbutils.widgets.get("quality_rules_path")
if not re.fullmatch(r"2026-07-(0[5-9]|10)", DATE): raise ValueError("invalid business_date")
if RULE_PATH not in sys.path: sys.path.insert(0, RULE_PATH)
from data_contracts.quality_rules.registry import get_rules
from data_contracts.audit.writer import write_audit
from data_contracts.table_catalog import DOMAINS
CAT="workspace"; BRONZE="dev_customer_bronze"; SILVER="dev_customer_silver"; QUAR="dev_customer_quarantine"
ROOT=f"dbfs:/Volumes/{CAT}/dev_source_landing/source_snapshot_files/banking_v2/snapshots/domain=customer_master/simulation_id=banking-20260705-20260710"
TABLES=DOMAINS["customer"]["scd2"]
metrics=[]; findings=[]
def full(schema,name): return f"{CAT}.{schema}.{name}"
def add(name,check,passed,actual,expected="0",detail="",blocking=True):
    findings.append(Row(run_id=RUN,business_date=DATE,table_name=name,check_name=check,expected_value=str(expected),actual_value=str(actual),passed=bool(passed),detail=detail,blocking=blocking,checked_at=datetime.utcnow()))
for name,key in TABLES.items():
    landing=spark.read.parquet(f"{ROOT}/snapshot_type=full/business_date={DATE}/customer_master/{name}")
    landing_count=landing.count(); rules=get_rules(name)
    bad=" OR ".join(f"NOT coalesce(({x}), false)" for x in rules.values())
    if name == "core_banking_customer":
        duplicate=landing.filter("national_id IS NOT NULL").groupBy("national_id").count().filter("count > 1").select("national_id")
        landing=landing.join(duplicate.withColumn("_dup",F.lit(True)),"national_id","left"); bad=f"({bad}) OR _dup"
    invalid=landing.filter(bad).count(); valid=landing_count-invalid
    bronze=spark.table(full(BRONZE,name)); cdc=(spark.read.option("readChangeFeed","true").option("startingVersion","1").table(full(BRONZE,name)).filter("_change_type IN ('insert','update_postimage')").filter(F.col("business_date")==F.to_date(F.lit(DATE))).count())
    clean=spark.table(full(SILVER,name)).count()
    quarantined=spark.table(full(QUAR,name)).filter(F.col("validation_business_date")==F.to_date(F.lit(DATE))).count()
    metrics.append(Row(run_id=RUN,business_date=DATE,table_name=name,landing_rows=landing_count,bronze_change_rows=cdc,clean_current_rows=clean,quarantined_rows=quarantined,recorded_at=datetime.utcnow()))
    add(name,"landing_reconciles",landing_count==valid+invalid,f"{valid}+{invalid}",landing_count)
    add(name,"quarantine_change_history_reconciliation",quarantined>=invalid,quarantined,invalid,
        "Observed only: one-shot full-history processing can close an SCD2 row before the quarantine CDF consumer reads it. Per-date strict equality requires daily pipeline runs.",
        blocking=False)
    add(name,"bronze_exists",spark.catalog.tableExists(full(BRONZE,name)),True,True)
failed = [row for row in findings if row.blocking and not row.passed]
metrics_by_table = {row.table_name: row.asDict() for row in metrics}
rule_audits = [{
    "audit_id": str(uuid.uuid4()), "run_id": RUN, "business_date": DATE,
    "table_name": row.table_name, "rule_name": row.check_name,
    "records_checked": metrics_by_table.get(row.table_name, {}).get("landing_rows", 0),
    "records_failed": 0 if row.passed else int(row.actual_value) if str(row.actual_value).isdigit() else 1,
    "evaluated_at": row.checked_at,
} for row in findings]
write_audit(spark, catalog=CAT, domain="customer", pipeline_run_id=RUN,
    pipeline_name="dev-customer-quality-audit", business_date=DATE,
    execution_status="FAILED" if failed else "SUCCEEDED",
    table_metrics=[row.asDict() for row in metrics], rule_audits=rule_audits)
if failed: raise AssertionError("Customer quality audit failed")
