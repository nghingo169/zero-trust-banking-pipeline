"""Create non-gating monitoring views over the native SDP event log."""

from __future__ import annotations

import re


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _principal(value: str) -> str:
    if not value or "`" in value:
        raise ValueError(f"Unsafe or empty principal: {value!r}")
    return f"`{value}`"


def _layer(expression: str) -> str:
    return f"""CASE
      WHEN lower({expression}) LIKE '%.silver_validated.%' THEN 'SILVER_VALIDATED'
      WHEN lower({expression}) LIKE '%.bronze.%' THEN 'BRONZE'
      WHEN lower({expression}) LIKE '%.silver.%' THEN 'SILVER'
      WHEN lower({expression}) LIKE '%.gold.%' THEN 'GOLD'
      WHEN lower({expression}) LIKE '%.governance.%' THEN 'GOVERNANCE'
      ELSE 'OTHER'
    END"""


def monitoring_view_statements(
    catalog: str,
    governance_schema: str,
    event_log_table: str,
) -> dict[str, str]:
    """Return idempotent view DDL keyed by dashboard data source name."""

    catalog = _identifier(catalog)
    governance_schema = _identifier(governance_schema)
    event_log_table = _identifier(event_log_table)
    prefix = f"`{catalog}`.`{governance_schema}`"
    events = f"{prefix}.`{event_log_table}`"
    runs = f"{prefix}.`pipeline_run`"
    quarantine = f"{prefix}.`silver_quarantine_record`"

    pipeline_updates = f"""
CREATE OR REPLACE VIEW {prefix}.`monitoring_pipeline_updates` AS
WITH last_status_per_update AS (
  SELECT
    CAST(origin.pipeline_id AS STRING) AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    CAST(origin.update_id AS STRING) AS pipeline_update_id,
    FROM_JSON(
      details,
      'struct<update_progress:struct<state:string>>'
    ).update_progress.state AS pipeline_update_status,
    timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY origin.update_id ORDER BY timestamp DESC
    ) AS row_number
  FROM {events}
  WHERE event_type = 'update_progress'
  QUALIFY row_number = 1
),
update_durations AS (
  SELECT
    CAST(origin.pipeline_id AS STRING) AS pipeline_id,
    CAST(origin.update_id AS STRING) AS pipeline_update_id,
    MIN(CASE WHEN event_type = 'create_update' THEN timestamp END) AS start_time,
    COALESCE(
      MAX(CASE
        WHEN event_type = 'update_progress'
         AND FROM_JSON(
           details,
           'struct<update_progress:struct<state:string>>'
         ).update_progress.state IN ('COMPLETED', 'FAILED', 'CANCELED', 'CANCELLED')
        THEN timestamp
      END),
      current_timestamp()
    ) AS end_time
  FROM {events}
  WHERE event_type IN ('create_update', 'update_progress')
    AND origin.update_id IS NOT NULL
  GROUP BY origin.pipeline_id, origin.update_id
  HAVING start_time IS NOT NULL
)
SELECT
  run.pipeline_run_id,
  run.business_date,
  status.pipeline_id,
  status.pipeline_name,
  status.pipeline_update_id,
  duration.start_time,
  duration.end_time,
  ROUND(
    TIMESTAMPDIFF(MILLISECOND, duration.start_time, duration.end_time) / 1000
  ) AS duration_seconds,
  status.pipeline_update_status,
  run.execution_status AS job_run_status
FROM last_status_per_update status
JOIN update_durations duration
  ON status.pipeline_id = duration.pipeline_id
 AND status.pipeline_update_id = duration.pipeline_update_id
LEFT JOIN {runs} run
  ON run.pipeline_id = status.pipeline_id
 AND run.pipeline_update_id = status.pipeline_update_id
""".strip()

    table_metrics = f"""
CREATE OR REPLACE VIEW {prefix}.`monitoring_table_metrics` AS
WITH flow_progress_raw AS (
  SELECT
    origin.pipeline_name AS pipeline_name,
    CAST(origin.pipeline_id AS STRING) AS pipeline_id,
    CAST(origin.update_id AS STRING) AS pipeline_update_id,
    origin.flow_name AS table_name,
    timestamp,
    details:flow_progress.status AS flow_status,
    TRY_CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)
      AS num_output_rows,
    TRY_CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT)
      AS num_upserted_rows,
    TRY_CAST(details:flow_progress.metrics.num_deleted_rows AS BIGINT)
      AS num_deleted_rows,
    TRY_CAST(details:flow_progress.data_quality.dropped_records AS BIGINT)
      AS num_expectation_dropped_rows
  FROM {events}
  WHERE event_type = 'flow_progress'
    AND origin.flow_name IS NOT NULL
    AND origin.flow_name != 'pipelines.flowTimeMetrics.missingFlowName'
),
aggregated_flows AS (
  SELECT
    pipeline_name,
    pipeline_id,
    pipeline_update_id,
    table_name,
    MIN(CASE WHEN flow_status IN ('STARTING', 'RUNNING', 'COMPLETED')
      THEN timestamp END) AS start_time,
    MAX(CASE WHEN flow_status IN ('STARTING', 'RUNNING', 'COMPLETED')
      THEN timestamp END) AS end_time,
    MAX_BY(flow_status, timestamp) FILTER (
      WHERE flow_status IN (
        'COMPLETED', 'FAILED', 'CANCELED', 'CANCELLED', 'EXCLUDED',
        'SKIPPED', 'STOPPED', 'IDLE'
      )
    ) AS flow_status,
    SUM(COALESCE(num_output_rows, 0)) AS output_rows,
    SUM(COALESCE(num_upserted_rows, 0)) AS upserted_rows,
    SUM(COALESCE(num_deleted_rows, 0)) AS deleted_rows,
    MAX(COALESCE(num_expectation_dropped_rows, 0)) AS dropped_rows
  FROM flow_progress_raw
  GROUP BY pipeline_name, pipeline_id, pipeline_update_id, table_name
)
SELECT
  run.pipeline_run_id,
  run.business_date,
  flow.pipeline_id,
  flow.pipeline_name,
  flow.pipeline_update_id,
  {_layer('flow.table_name')} AS layer,
  flow.table_name,
  flow.flow_status,
  flow.start_time,
  flow.end_time,
  ROUND(
    TIMESTAMPDIFF(MILLISECOND, flow.start_time, flow.end_time) / 1000
  ) AS duration_seconds,
  flow.output_rows,
  flow.upserted_rows,
  flow.deleted_rows,
  flow.dropped_rows
FROM aggregated_flows flow
LEFT JOIN {runs} run
  ON run.pipeline_id = flow.pipeline_id
 AND run.pipeline_update_id = flow.pipeline_update_id
""".strip()

    rule_metrics = f"""
CREATE OR REPLACE VIEW {prefix}.`monitoring_rule_metrics` AS
WITH expectation_events AS (
  SELECT
    CAST(origin.pipeline_id AS STRING) AS pipeline_id,
    CAST(origin.update_id AS STRING) AS pipeline_update_id,
    timestamp AS evaluated_at,
    EXPLODE(FROM_JSON(
      details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    )) AS expectation
  FROM {events}
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
),
native_expectations AS (
  SELECT
    run.pipeline_run_id,
    run.business_date,
    event.pipeline_id,
    event.pipeline_update_id,
    {_layer('event.expectation.dataset')} AS layer,
    event.expectation.dataset AS dataset_name,
    event.expectation.name AS rule_name,
    'SDP_EXPECTATION' AS metric_source,
    SUM(event.expectation.passed_records) AS passed_records,
    SUM(event.expectation.failed_records) AS failed_records,
    SUM(event.expectation.failed_records) AS rejected_records,
    MAX(event.evaluated_at) AS evaluated_at
  FROM expectation_events event
  LEFT JOIN {runs} run
    ON run.pipeline_id = event.pipeline_id
   AND run.pipeline_update_id = event.pipeline_update_id
  GROUP BY
    run.pipeline_run_id,
    run.business_date,
    event.pipeline_id,
    event.pipeline_update_id,
    event.expectation.dataset,
    event.expectation.name
),
quarantine_rules AS (
  SELECT
    quarantine.pipeline_run_id,
    run.business_date,
    run.pipeline_id,
    run.pipeline_update_id,
    'SILVER_VALIDATED' AS layer,
    quarantine.source_table_name AS dataset_name,
    quarantine.failed_rule_name AS rule_name,
    'SILVER_QUARANTINE' AS metric_source,
    CAST(NULL AS BIGINT) AS passed_records,
    COUNT(*) AS failed_records,
    COUNT(*) AS rejected_records,
    MAX(quarantine.quarantined_at) AS evaluated_at
  FROM {quarantine} quarantine
  LEFT JOIN {runs} run
    ON run.pipeline_run_id = quarantine.pipeline_run_id
  GROUP BY
    quarantine.pipeline_run_id,
    run.business_date,
    run.pipeline_id,
    run.pipeline_update_id,
    quarantine.source_table_name,
    quarantine.failed_rule_name
)
SELECT * FROM native_expectations
UNION ALL
SELECT * FROM quarantine_rules
""".strip()

    return {
        "monitoring_pipeline_updates": pipeline_updates,
        "monitoring_table_metrics": table_metrics,
        "monitoring_rule_metrics": rule_metrics,
    }


def create_monitoring_views(
    spark,
    *,
    catalog: str,
    governance_schema: str,
    event_log_table: str,
    data_engineer_group: str,
) -> list[str]:
    """Create and grant dashboard views, returning non-fatal setup errors."""

    principal = _principal(data_engineer_group)
    failures: list[str] = []
    for view_name, statement in monitoring_view_statements(
        catalog, governance_schema, event_log_table
    ).items():
        try:
            spark.sql(statement)
            spark.sql(
                f"GRANT SELECT ON TABLE `{catalog}`.`{governance_schema}`."
                f"`{view_name}` TO {principal}"
            )
        except Exception as exc:  # Monitoring must never change the pipeline outcome.
            failures.append(f"{view_name}: {exc}")
    return failures
