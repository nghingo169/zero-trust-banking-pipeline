# Databricks notebook source
"""Finalize operational status and link the native SDP update diagnostics."""


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


def literal(value: str) -> str:
    return value.replace("'", "''")


RUN_ID = widget("run_id", "")
PIPELINE_NAME = widget("pipeline_name", "banking-investigation-pipeline")
STATUS = widget("status", "FAILED")
CATALOG = widget("catalog", "workspace")
GOVERNANCE_SCHEMA = widget("governance_schema", "governance")
EVENT_LOG_TABLE = widget(
    "event_log_table", "banking_investigation_pipeline_event_log"
)
FAIL_AFTER_RECORDING = widget("fail_after_recording", "false").lower() == "true"

if not RUN_ID:
    raise ValueError("run_id is required")
if STATUS not in {"SUCCEEDED", "FAILED"}:
    raise ValueError("status must be SUCCEEDED or FAILED")

run_id = literal(RUN_ID)
pipeline_name = literal(PIPELINE_NAME)
run_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.pipeline_run"
event_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.{EVENT_LOG_TABLE}"

native_rows = []
if spark.catalog.tableExists(event_table):
    native_rows = spark.sql(
        f"""
        SELECT
          CAST(origin.pipeline_id AS STRING) AS pipeline_id,
          CAST(origin.update_id AS STRING) AS pipeline_update_id
        FROM {event_table}
        WHERE event_type = 'create_update'
          AND origin.pipeline_name = '{pipeline_name}'
          AND timestamp >= (
            SELECT start_time FROM {run_table}
            WHERE pipeline_run_id = '{run_id}'
              AND pipeline_name = '{pipeline_name}'
          )
        ORDER BY timestamp DESC
        LIMIT 1
        """
    ).collect()

pipeline_id = literal(native_rows[0]["pipeline_id"]) if native_rows else None
pipeline_update_id = (
    literal(native_rows[0]["pipeline_update_id"]) if native_rows else None
)
pipeline_id_sql = f"'{pipeline_id}'" if pipeline_id else "NULL"
pipeline_update_id_sql = f"'{pipeline_update_id}'" if pipeline_update_id else "NULL"

spark.sql(
    f"""
    UPDATE {run_table}
    SET execution_status = '{STATUS}',
        end_time = current_timestamp(),
        pipeline_id = {pipeline_id_sql},
        pipeline_update_id = {pipeline_update_id_sql}
    WHERE pipeline_run_id = '{run_id}'
      AND pipeline_name = '{pipeline_name}'
    """
)

if STATUS == "SUCCEEDED" and not pipeline_update_id:
    spark.sql(
        f"""
        UPDATE {run_table}
        SET execution_status = 'FAILED'
        WHERE pipeline_run_id = '{run_id}' AND pipeline_name = '{pipeline_name}'
        """
    )
    raise RuntimeError("Successful SDP update has no matching native event-log update ID")
if FAIL_AFTER_RECORDING:
    raise RuntimeError(f"Pipeline job failed; governance evidence retained for {RUN_ID}")
print(f"Finalized {RUN_ID} as {STATUS} with SDP update {pipeline_update_id}.")
