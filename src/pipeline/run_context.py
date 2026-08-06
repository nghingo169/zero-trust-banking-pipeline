"""Fail-closed operational lineage resolution for the Source-to-Gold SDP graph."""

from __future__ import annotations

import re

CANONICAL_PIPELINE_NAME = "banking-investigation-pipeline"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(value: str, label: str) -> str:
    if not value or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def active_run_id_sql(
    catalog: str,
    governance_schema: str = "governance",
    pipeline_name: str = CANONICAL_PIPELINE_NAME,
) -> str:
    """Return a scalar SQL expression that accepts exactly one active context.

    ``raise_error`` makes missing and ambiguous contexts fail the SDP update. No
    synthetic lineage value is generated and no historical run is selected.
    """

    schema = _identifier(governance_schema, "governance schema")
    table_ref = (
        f"{_identifier(catalog, 'catalog')}.{schema}.pipeline_run"
        if catalog
        else f"{schema}.pipeline_run"
    )
    escaped_name = pipeline_name.replace("'", "''")
    return f"""(
        SELECT CASE
          WHEN COUNT(*) = 1 THEN MAX(CAST(pipeline_run_id AS STRING))
          ELSE raise_error(CONCAT(
            'Expected exactly one RUNNING context for {escaped_name}; found ',
            CAST(COUNT(*) AS STRING)
          ))
        END
        FROM {table_ref}
        WHERE pipeline_name = '{escaped_name}'
          AND execution_status = 'RUNNING'
    )"""


def pipeline_run_id_column(
    functions,
    dataframe,
    *,
    catalog: str,
    governance_schema: str = "governance",
    pipeline_name: str = CANONICAL_PIPELINE_NAME,
):
    """Return existing lineage or resolve the one active operational context."""

    if "pipeline_run_id" in dataframe.columns:
        return functions.col("pipeline_run_id").cast("string").alias("pipeline_run_id")
    return (
        functions.expr(active_run_id_sql(catalog, governance_schema, pipeline_name))
        .cast("string")
        .alias("pipeline_run_id")
    )
