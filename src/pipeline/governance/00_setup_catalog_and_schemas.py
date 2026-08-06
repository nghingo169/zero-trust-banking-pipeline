# Databricks notebook source
"""Create greenfield catalog objects and least-privilege execution grants."""

import re
import sys


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def principal(value: str) -> str:
    if not value or "`" in value:
        raise ValueError(f"Unsafe or empty principal: {value!r}")
    return f"`{value}`"


CATALOG = identifier(widget("catalog", "workspace"))
SOURCE_LANDING_SCHEMA = identifier(widget("source_landing_schema", "source_landing"))
SOURCE_LANDING_VOLUME = identifier(widget("source_landing_volume", "source_snapshot_files"))
SOURCE_MODE = widget("source_mode", "volume")
BRONZE_SCHEMA = identifier(widget("bronze_schema", "bronze"))
VALIDATED_SCHEMA = identifier(widget("silver_validated_schema", "silver_validated"))
SILVER_SCHEMA = identifier(widget("silver_schema", "silver"))
GOLD_SCHEMA = identifier(widget("gold_schema", "gold"))
GOVERNANCE_SCHEMA = identifier(widget("governance_schema", "governance"))
PIPELINE_SP = principal(widget("pipeline_service_principal_name", ""))
GOVERNANCE_ADMINS = principal(widget("governance_admin_group", "governance-admins"))
DATA_ENGINEERS = principal(widget("data_engineer_group", "data-engineers"))
PII_DQ_OPERATORS = principal(widget("pii_dq_operator_group", "pii-dq-operator"))
SOURCE_PATH = widget("source_path", "")

if SOURCE_PATH and SOURCE_PATH not in sys.path:
    sys.path.insert(0, SOURCE_PATH)

from data_contracts.audit.writer import ensure_audit_tables

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in (
    SOURCE_LANDING_SCHEMA,
    BRONZE_SCHEMA,
    VALIDATED_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    GOVERNANCE_SCHEMA,
):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

if SOURCE_MODE == "volume":
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SOURCE_LANDING_SCHEMA}.{SOURCE_LANDING_VOLUME}"
    )
elif SOURCE_MODE != "s3":
    raise ValueError("source_mode must be 'volume' or 's3'")

ensure_audit_tables(spark, CATALOG, GOVERNANCE_SCHEMA)

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {PIPELINE_SP}")
for schema in (
    SOURCE_LANDING_SCHEMA,
    BRONZE_SCHEMA,
    VALIDATED_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    GOVERNANCE_SCHEMA,
):
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO {PIPELINE_SP}")
    spark.sql(
        f"GRANT CREATE TABLE, CREATE MATERIALIZED VIEW, MODIFY, SELECT "
        f"ON SCHEMA {CATALOG}.{schema} TO {PIPELINE_SP}"
    )

if SOURCE_MODE == "volume":
    spark.sql(
        f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME "
        f"{CATALOG}.{SOURCE_LANDING_SCHEMA}.{SOURCE_LANDING_VOLUME} TO {PIPELINE_SP}"
    )

# Account groups receive object privileges; raw visibility still requires JIT
# membership in pii-dq-operator, which is managed and audited outside the Bundle.
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {DATA_ENGINEERS}")
for schema in (SILVER_SCHEMA, GOLD_SCHEMA, GOVERNANCE_SCHEMA):
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO {DATA_ENGINEERS}")
for schema in (SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO {DATA_ENGINEERS}")

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {PII_DQ_OPERATORS}")
for schema in (BRONZE_SCHEMA, VALIDATED_SCHEMA, GOVERNANCE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO {PII_DQ_OPERATORS}")
    spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO {PII_DQ_OPERATORS}")

spark.sql(f"GRANT USE CATALOG, MANAGE ON CATALOG {CATALOG} TO {GOVERNANCE_ADMINS}")
print(f"Catalog and schema prerequisites are ready in {CATALOG}.")
