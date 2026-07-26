"""Persist centralized run, table, and rule-level pipeline audit records."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Mapping


AUDIT_SCHEMA = "dev_quality_audit"


def _table(catalog: str, name: str) -> str:
    return f"{catalog}.{AUDIT_SCHEMA}.{name}"


def ensure_audit_tables(spark, catalog: str) -> None:
    """Create the shared audit storage once, without resetting prior evidence."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{AUDIT_SCHEMA}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'pipeline_run')} (
            pipeline_run_id STRING,
            pipeline_name STRING,
            domain STRING,
            business_date DATE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            execution_status STRING
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'table_quality_metrics')} (
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
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'data_quality_audit_log')} (
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
) -> None:
    """Append immutable evidence for one post-Silver audit task execution."""

    ensure_audit_tables(spark, catalog)
    now = datetime.utcnow()
    spark.createDataFrame(
        [(pipeline_run_id, pipeline_name, domain, date.fromisoformat(business_date), now, now, execution_status)],
        "pipeline_run_id string, pipeline_name string, domain string, business_date date, "
        "start_time timestamp, end_time timestamp, execution_status string",
    ).write.mode("append").saveAsTable(_table(catalog, "pipeline_run"))

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
        ).write.mode("append").saveAsTable(_table(catalog, "table_quality_metrics"))

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
        ).write.mode("append").saveAsTable(_table(catalog, "data_quality_audit_log"))
