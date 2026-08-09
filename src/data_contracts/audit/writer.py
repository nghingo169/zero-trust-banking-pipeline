"""Create governance tables that are not supplied by the native SDP event log."""

from __future__ import annotations

DEFAULT_GOVERNANCE_SCHEMA = "governance"


def _table(
    catalog: str,
    name: str,
    governance_schema: str = DEFAULT_GOVERNANCE_SCHEMA,
) -> str:
    return f"{catalog}.{governance_schema}.{name}"


def ensure_governance_tables(
    spark, catalog: str, governance_schema: str = DEFAULT_GOVERNANCE_SCHEMA
) -> None:
    """Create operational lineage storage once, without resetting prior evidence.

    SDP update, flow, row-count, and expectation metrics come from the native
    pipeline event log. Legacy custom metric tables are not recreated.
    """

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
        f"""CREATE TABLE IF NOT EXISTS {_table(catalog, 'pii_masking_log', governance_schema)} (
            audit_id STRING,
            pipeline_run_id STRING,
            target_table_name STRING,
            target_column_name STRING,
            masking_policy STRING,
            records_transformed BIGINT,
            executed_at TIMESTAMP
        ) USING DELTA"""
    )
