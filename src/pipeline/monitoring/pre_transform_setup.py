# Databricks notebook source
"""Initialize the one operational context consumed by the Source-to-Gold SDP."""

import sys


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


def literal(value: str) -> str:
    return value.replace("'", "''")


BUSINESS_DATE = widget("business_date", "2026-07-10")
RUN_ID = widget("run_id", "")
PIPELINE_NAME = widget("pipeline_name", "banking-investigation-pipeline")
CATALOG = widget("catalog", "workspace")
GOVERNANCE_SCHEMA = widget("governance_schema", "governance")
SOURCE_PATH = widget("source_path", "")

if not RUN_ID:
    raise ValueError("run_id is required from the parent Lakeflow Job")
if SOURCE_PATH and SOURCE_PATH not in sys.path:
    sys.path.insert(0, SOURCE_PATH)

from data_contracts.audit.writer import ensure_governance_tables

ensure_governance_tables(spark, CATALOG, GOVERNANCE_SCHEMA)
table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.pipeline_run"
run_id = literal(RUN_ID)
pipeline_name = literal(PIPELINE_NAME)
business_date = literal(BUSINESS_DATE)

spark.sql(
    f"""
    UPDATE {table}
    SET execution_status = 'ABANDONED', end_time = current_timestamp()
    WHERE pipeline_name = '{pipeline_name}'
      AND execution_status = 'RUNNING'
      AND pipeline_run_id <> '{run_id}'
    """
)

spark.sql(
    f"""
    MERGE INTO {table} AS target
    USING (
      SELECT
        '{run_id}' AS pipeline_run_id,
        '{pipeline_name}' AS pipeline_name,
        'ALL' AS domain,
        CAST('{business_date}' AS DATE) AS business_date,
        current_timestamp() AS start_time,
        CAST(NULL AS TIMESTAMP) AS end_time,
        'RUNNING' AS execution_status,
        CAST(NULL AS STRING) AS pipeline_update_id,
        CAST(NULL AS STRING) AS pipeline_id
    ) AS source
    ON target.pipeline_run_id = source.pipeline_run_id
       AND target.pipeline_name = source.pipeline_name
    WHEN MATCHED THEN UPDATE SET
      target.business_date = source.business_date,
      target.start_time = source.start_time,
      target.end_time = source.end_time,
      target.execution_status = source.execution_status,
      target.pipeline_update_id = source.pipeline_update_id,
      target.pipeline_id = source.pipeline_id
    WHEN NOT MATCHED THEN INSERT (
      pipeline_run_id, pipeline_name, domain, business_date, start_time,
      end_time, execution_status, pipeline_update_id, pipeline_id
    ) VALUES (
      source.pipeline_run_id, source.pipeline_name, source.domain,
      source.business_date, source.start_time, source.end_time,
      source.execution_status, source.pipeline_update_id, source.pipeline_id
    )
    """
)

active_count = spark.sql(
    f"""
    SELECT COUNT(*) AS active_count
    FROM {table}
    WHERE pipeline_name = '{pipeline_name}' AND execution_status = 'RUNNING'
    """
).first()["active_count"]
if active_count != 1:
    raise RuntimeError(f"Expected exactly one active run context, found {active_count}")
print(f"Initialized RUNNING context {RUN_ID} for {PIPELINE_NAME}.")
