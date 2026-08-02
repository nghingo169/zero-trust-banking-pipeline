# Databricks notebook source
"""Pre-transform setup notebook.

Gets the official job.run_id from Workflow parameters, creates/overwrites 
the active_run_context table in Governance, so downstream DLT Pipelines 
can read the exact job_run_id directly via Spark SQL without relying on Spark Conf.
"""

import sys
from datetime import datetime

# Widgets receiving parameters directly from Workflow Job
dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "workspace")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
CATALOG = dbutils.widgets.get("catalog")

if not RUN_ID:
    raise ValueError("run_id is required from Workflow")

print(f"==================================================")
print(f"INITIALIZING TRANSFORM RUN CONTEXT")
print(f"Official Job Run ID : {RUN_ID}")
print(f"Business Date       : {BUSINESS_DATE}")
print(f"==================================================")

# 1. Tạo/Ghi đè bảng Active Context duy nhất 1 dòng để DLT Pipeline đọc
CONTEXT_TABLE = f"{CATALOG}.governance.active_run_context"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONTEXT_TABLE} (
        active_run_id STRING,
        business_date STRING,
        updated_at TIMESTAMP
    )
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CONTEXT_TABLE} AS
    SELECT 
        '{RUN_ID}' AS active_run_id,
        '{BUSINESS_DATE}' AS business_date,
        current_timestamp() AS updated_at
""")

print(f"Active run context successfully written to {CONTEXT_TABLE}.")

# 2. Log trạng thái RUNNING vào pipeline_execution_log như cũ
INIT_AUDIT_SQL = f"""
MERGE INTO {CATALOG}.governance.pipeline_execution_log AS target
USING (
    SELECT 
        '{RUN_ID}' AS run_id,
        '{BUSINESS_DATE}' AS business_date,
        'transform_silver_atomic' AS pipeline_name,
        'RUNNING' AS status,
        current_timestamp() AS started_at
) AS source
ON target.run_id = source.run_id AND target.pipeline_name = source.pipeline_name
WHEN MATCHED THEN UPDATE SET target.status = source.status
WHEN NOT MATCHED THEN INSERT (run_id, business_date, pipeline_name, status, started_at)
VALUES (source.run_id, source.business_date, source.pipeline_name, source.status, source.started_at)
"""

try:
    spark.sql(INIT_AUDIT_SQL)
    print("Governance execution log updated to RUNNING.")
except Exception as e:
    print(f"Governance table update skipped/failed: {e}")