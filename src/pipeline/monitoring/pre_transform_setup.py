# Databricks notebook source
"""Pre-transform setup notebook.

Registers the start of the transform run session in governance.pipeline_run.
"""

import sys
from datetime import datetime

# Receiving parameters directly from Workflow Job
dbutils.widgets.text("business_date", "2026-07-10")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "workspace")

BUSINESS_DATE = dbutils.widgets.get("business_date")
RUN_ID = dbutils.widgets.get("run_id")
CATALOG = dbutils.widgets.get("catalog")

if not RUN_ID:
    raise ValueError("run_id is required from Workflow")

GOVERNANCE_TABLE = f"{CATALOG}.governance.pipeline_run"

# Ghi nhận log khởi tạo vào đúng schema của bảng governance.pipeline_run
INIT_AUDIT_SQL = f"""
MERGE INTO {GOVERNANCE_TABLE} AS target
USING (
    SELECT 
        '{RUN_ID}' AS pipeline_run_id,
        'full-pipeline' AS pipeline_name,
        'ALL' AS domain,
        '{BUSINESS_DATE}' AS business_date,
        current_timestamp() AS start_time,
        CAST(NULL AS TIMESTAMP) AS end_time,
        'RUNNING' AS execution_status
) AS source
ON target.pipeline_run_id = source.pipeline_run_id 
   AND target.pipeline_name = source.pipeline_name
WHEN MATCHED THEN 
    UPDATE SET 
        target.execution_status = source.execution_status,
        target.start_time = source.start_time
WHEN NOT MATCHED THEN 
    INSERT (pipeline_run_id, pipeline_name, domain, business_date, start_time, end_time, execution_status)
    VALUES (source.pipeline_run_id, source.pipeline_name, source.domain, source.business_date, source.start_time, source.end_time, source.execution_status)
"""

try:
    spark.sql(INIT_AUDIT_SQL)
    print(f"Successfully logged pipeline_run_id {RUN_ID} as RUNNING in {GOVERNANCE_TABLE}.")
except Exception as e:
    print(f"Governance table update skipped/failed: {e}")