# Databricks notebook source
"""Apply governed PII tags after publication and expose redacted DQ evidence."""


def widget(name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name)


def principal(value: str) -> str:
    if not value or "`" in value:
        raise ValueError(f"Unsafe or empty principal: {value!r}")
    return f"`{value}`"


CATALOG = widget("catalog", "workspace")
SILVER = widget("silver_schema", "silver")
GOLD = widget("gold_schema", "gold")
GOVERNANCE = widget("governance_schema", "governance")
DATA_ENGINEERS = principal(widget("data_engineer_group", "data-engineers"))

TAGS = (
    (SILVER, "party_profile_version", "full_name", "individual_name"),
    (SILVER, "party_profile_version", "address", "address"),
    (SILVER, "party_identifier", "identifier_value", "national_id"),
    (SILVER, "party_kyc_assessment", "id_number", "national_id"),
    (SILVER, "party_employment", "employer_name", "org_name"),
    (SILVER, "party_service_request", "request_description", "narrative"),
    (SILVER, "payment_card", "card_number", "card_number"),
    (SILVER, "merchant_location", "store_name", "store_name"),
    (SILVER, "merchant_location", "store_address", "address"),
    (SILVER, "call_center_contact", "caller_phone", "phone"),
    (SILVER, "call_center_contact", "call_reason", "narrative"),
    (SILVER, "investigation_note", "note_text", "narrative"),
    (SILVER, "watchlist_entry", "entity_name", "individual_name"),
    (SILVER, "sanctions_screening", "screened_name", "individual_name"),
    (SILVER, "gateway_payment", "payment_reference", "narrative"),
    (GOLD, "ai_aml_investigation_context", "latest_note_text", "narrative"),
    (GOLD, "ai_customer_360_context", "employer_name", "org_name"),
    (GOLD, "ai_customer_360_context", "last_call_reason", "narrative"),
)

for schema, table, column, tag_value in TAGS:
    spark.sql(
        f"ALTER TABLE {CATALOG}.{schema}.{table} ALTER COLUMN {column} "
        f"SET TAGS ('pii_type' = '{tag_value}')"
    )

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOVERNANCE}.silver_quarantine_redacted_evidence AS
    SELECT
      quarantine_key,
      pipeline_run_id,
      source_table_name,
      sha2(source_business_key, 256) AS source_record_token,
      failed_rule_name,
      quarantine_reason,
      quarantined_at
    FROM {CATALOG}.{GOVERNANCE}.silver_quarantine_record
    """
)

for object_name in (
    "pipeline_run",
    "table_quality_metrics",
    "data_quality_audit_log",
    "pii_masking_log",
    "silver_quarantine_redacted_evidence",
):
    spark.sql(
        f"GRANT SELECT ON TABLE {CATALOG}.{GOVERNANCE}.{object_name} TO {DATA_ENGINEERS}"
    )

expected = len(TAGS)
actual = spark.sql(
    f"""
    SELECT COUNT(*) AS tagged_columns
    FROM {CATALOG}.information_schema.column_tags
    WHERE tag_name = 'pii_type'
      AND schema_name IN ('{SILVER}', '{GOLD}')
    """
).first()["tagged_columns"]
if actual < expected:
    raise RuntimeError(f"Expected at least {expected} PII tags, found {actual}")
print(f"Verified {actual} PII-tagged columns and the redacted quarantine view.")
