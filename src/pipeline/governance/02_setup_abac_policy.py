# Databricks notebook source
"""Create the catalog ABAC mask before data is published."""


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


def principal(value: str) -> str:
    if not value or "`" in value:
        raise ValueError(f"Unsafe or empty principal: {value!r}")
    return f"`{value}`"


CATALOG = widget("catalog", "workspace")
PIPELINE_SP = principal(widget("pipeline_service_principal_name", ""))
GOVERNANCE_ADMINS = principal(widget("governance_admin_group", "governance-admins"))
PII_DQ_OPERATOR = principal(widget("pii_dq_operator_group", "pii-dq-operator"))

spark.sql(f"""
    CREATE OR REPLACE POLICY abac_tdm_protection_policy
    ON CATALOG {CATALOG}
    COLUMN MASK {CATALOG}.governance.tdm_masking_engine
    TO `account users`
    EXCEPT {PIPELINE_SP}, {GOVERNANCE_ADMINS}, {PII_DQ_OPERATOR}
    FOR TABLES
    MATCH COLUMNS has_tag('pii_type') AS target_col
    ON COLUMN target_col
    USING COLUMNS (get_column_tag_value(target_col, 'pii_type'))
    """)

verification = spark.sql(f"""
    SELECT policy_name, policy_type, catalog_name, to_principals, except_principals
    FROM {CATALOG}.information_schema.abac_policy_definitions
    WHERE policy_name = 'abac_tdm_protection_policy'
    """).collect()
if len(verification) != 1:
    raise RuntimeError("ABAC policy metadata verification failed")
print(
    "ABAC policy is active with variable-driven pipeline, governance-admin, and JIT exceptions."
)
