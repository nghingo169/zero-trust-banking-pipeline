"""Persist centralized run, table, and rule-level pipeline audit records."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Mapping

AUDIT_SCHEMA = "governance"


def _table(catalog: str, name: str, governance_schema: str = AUDIT_SCHEMA) -> str:
    return f"{catalog}.{governance_schema}.{name}"


def ensure_audit_tables(
    spark, catalog: str, governance_schema: str = AUDIT_SCHEMA
) -> None:
    """Create the shared audit storage once, without resetting prior evidence."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{governance_schema}")
    pipeline_run_table = _table(catalog, "pipeline_run", governance_schema)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline_run_table} (
            pipeline_run_id STRING,
            pipeline_name STRING,
            domain STRING,
            business_date DATE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            execution_status STRING,
            pipeline_update_id STRING,
            pipeline_id STRING
        ) USING DELTA""")
    existing_columns = set(spark.table(pipeline_run_table).columns)
    for column_name in ("pipeline_update_id", "pipeline_id"):
        if column_name not in existing_columns:
            spark.sql(
                f"ALTER TABLE {pipeline_run_table} ADD COLUMNS ({column_name} STRING)"
            )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'table_quality_metrics', governance_schema)} (
            pipeline_run_id STRING,
            domain STRING,
            business_date DATE,
            table_name STRING,
            landing_rows BIGINT,
            bronze_change_rows BIGINT,
            clean_current_rows BIGINT,
            quarantined_rows BIGINT,
            recorded_at TIMESTAMP
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'data_quality_audit_log', governance_schema)} (
            audit_id STRING,
            pipeline_run_id STRING,
            domain STRING,
            business_date DATE,
            target_table_name STRING,
            rule_name STRING,
            records_checked BIGINT,
            records_failed BIGINT,
            evaluated_at TIMESTAMP
        ) USING DELTA"""
    )
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'pii_masking_log', governance_schema)} (
            audit_id STRING,
            pipeline_run_id STRING,
            target_table_name STRING,
            target_column_name STRING,
            masking_policy STRING,
            records_transformed BIGINT,
            executed_at TIMESTAMP
        ) USING DELTA""")


def write_audit(
    spark,
    *,
    catalog: str,
    domain: str,
    pipeline_run_id: str,
    pipeline_name: str,
    business_date: str,
    execution_status: str,
    table_metrics: Iterable[Mapping[str, object]],
    rule_audits: Iterable[Mapping[str, object]],
    governance_schema: str = AUDIT_SCHEMA,
) -> None:
    """Append immutable evidence for one post-Silver audit task execution."""

    ensure_audit_tables(spark, catalog, governance_schema)
    now = datetime.utcnow()
    pipeline_run = spark.createDataFrame(
        [
            (
                pipeline_run_id,
                pipeline_name,
                "ALL",
                date.fromisoformat(business_date),
                now,
                now,
                execution_status,
                None,
                None,
            )
        ],
        "pipeline_run_id string, pipeline_name string, domain string, business_date date, "
        "start_time timestamp, end_time timestamp, execution_status string, "
        "pipeline_update_id string, pipeline_id string",
    )
    pipeline_run.createOrReplaceTempView("_pipeline_run_to_upsert")
    spark.sql(f"""MERGE INTO {_table(catalog, 'pipeline_run', governance_schema)} AS target
            USING _pipeline_run_to_upsert AS source
            ON target.pipeline_run_id = source.pipeline_run_id
            WHEN NOT MATCHED THEN INSERT (
              pipeline_run_id, pipeline_name, domain, business_date,
              start_time, end_time, execution_status, pipeline_update_id, pipeline_id
            ) VALUES (
              source.pipeline_run_id, source.pipeline_name, source.domain,
              source.business_date, source.start_time, source.end_time,
              source.execution_status, source.pipeline_update_id, source.pipeline_id
            )""")

    metrics = list(table_metrics)
    if metrics:
        spark.createDataFrame(metrics).selectExpr(
            "run_id AS pipeline_run_id",
            f"'{domain}' AS domain",
            "CAST(business_date AS DATE) AS business_date",
            "table_name",
            "CAST(landing_rows AS BIGINT) AS landing_rows",
            "CAST(bronze_change_rows AS BIGINT) AS bronze_change_rows",
            "CAST(clean_current_rows AS BIGINT) AS clean_current_rows",
            "CAST(quarantined_rows AS BIGINT) AS quarantined_rows",
            "recorded_at",
        ).write.mode("append").saveAsTable(
            _table(catalog, "table_quality_metrics", governance_schema)
        )

    audits = list(rule_audits)
    if audits:
        spark.createDataFrame(audits).selectExpr(
            "audit_id",
            "run_id AS pipeline_run_id",
            f"'{domain}' AS domain",
            "CAST(business_date AS DATE) AS business_date",
            "table_name AS target_table_name",
            "rule_name",
            "CAST(records_checked AS BIGINT) AS records_checked",
            "CAST(records_failed AS BIGINT) AS records_failed",
            "evaluated_at",
        ).write.mode("append").saveAsTable(
            _table(catalog, "data_quality_audit_log", governance_schema)
        )
