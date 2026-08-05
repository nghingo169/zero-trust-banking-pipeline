# Databricks notebook source
# =============================================================================
# ZERO-TRUST BANKING PIPELINE - ABAC POLICY ENFORCEMENT (PYTHON NOTEBOOK)
# Description: Applies PII tags to Silver & Gold clean columns and registers Catalog ABAC Policy.
# Author: Data Engineering Squad
# =============================================================================

# Lấy cấu hình Schemas & Catalog từ Spark Environment
try:
    catalog = spark.conf.get("pipeline.catalog", "workspace")
    silver = spark.conf.get("pipeline.silver_schema", "silver")
    gold = spark.conf.get("pipeline.gold_schema", "gold")
except Exception:
    catalog = "workspace"
    silver = "silver"
    gold = "gold"

print(f"Applying Tags and Policies on Catalog: {catalog}, Silver: {silver}, Gold: {gold}")

# -----------------------------------------------------------------------------
# BƯỚC A: GÁN GOVERNED TAGS CHO CÁC CỘT PII TẠI TẦNG SILVER ATOMIC MODEL
# -----------------------------------------------------------------------------
# 1. Customer Domain
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_profile_version ALTER COLUMN full_name SET TAGS ('pii_type' = 'individual_name')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_profile_version ALTER COLUMN address SET TAGS ('pii_type' = 'address')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_identifier ALTER COLUMN identifier_value SET TAGS ('pii_type' = 'national_id')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_kyc_assessment ALTER COLUMN id_number SET TAGS ('pii_type' = 'national_id')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_employment ALTER COLUMN employer_name SET TAGS ('pii_type' = 'org_name')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.party_service_request ALTER COLUMN request_description SET TAGS ('pii_type' = 'narrative')")

# 2. Card & Merchant Domain
spark.sql(f"ALTER TABLE {catalog}.{silver}.payment_card ALTER COLUMN card_number SET TAGS ('pii_type' = 'card_number')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.merchant_location ALTER COLUMN store_name SET TAGS ('pii_type' = 'store_name')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.merchant_location ALTER COLUMN store_address SET TAGS ('pii_type' = 'address')")

# 3. Financial Crime & Call Center Domain
spark.sql(f"ALTER TABLE {catalog}.{silver}.call_center_contact ALTER COLUMN caller_phone SET TAGS ('pii_type' = 'phone')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.call_center_contact ALTER COLUMN call_reason SET TAGS ('pii_type' = 'narrative')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.investigation_note ALTER COLUMN note_text SET TAGS ('pii_type' = 'narrative')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.watchlist_entry ALTER COLUMN entity_name SET TAGS ('pii_type' = 'individual_name')")
spark.sql(f"ALTER TABLE {catalog}.{silver}.sanctions_screening ALTER COLUMN screened_name SET TAGS ('pii_type' = 'individual_name')")

# 4. Transaction Domain
spark.sql(f"ALTER TABLE {catalog}.{silver}.gateway_payment ALTER COLUMN payment_reference SET TAGS ('pii_type' = 'narrative')")

# -----------------------------------------------------------------------------
# BƯỚC B: GÁN GOVERNED TAGS CHO CÁC CỘT PII TẠI TẦNG GOLD (AI-READY CONTEXT)
# -----------------------------------------------------------------------------
spark.sql(f"ALTER TABLE {catalog}.{gold}.ai_aml_investigation_context ALTER COLUMN latest_note_text SET TAGS ('pii_type' = 'narrative')")
spark.sql(f"ALTER TABLE {catalog}.{gold}.ai_customer_360_context ALTER COLUMN employer_name SET TAGS ('pii_type' = 'org_name')")
spark.sql(f"ALTER TABLE {catalog}.{gold}.ai_customer_360_context ALTER COLUMN last_call_reason SET TAGS ('pii_type' = 'narrative')")

# -----------------------------------------------------------------------------
# BƯỚC C: ĐĂNG KÝ / KÍCH HOẠT ABAC COLUMN MASK POLICY TRÊN CATALOG
# -----------------------------------------------------------------------------
policy_script = f"""
CREATE OR REPLACE POLICY abac_tdm_protection_policy
ON CATALOG {catalog}
COLUMN MASK {catalog}.governance.tdm_masking_engine
TO `account users` 
EXCEPT `nghi.ngotieu@gmail.com`
FOR TABLES
MATCH COLUMNS has_tag('pii_type') AS target_col
ON COLUMN target_col
USING COLUMNS (get_column_tag_value(target_col, 'pii_type'));
"""
spark.sql(policy_script)
print("Successfully registered and enforced ABAC Dynamic Column Masking Policy.")

# -----------------------------------------------------------------------------
# BƯỚC D: AUDIT CHECK (Kiểm tra Metadata trong information_schema)
# -----------------------------------------------------------------------------
try:
    audit_df = spark.sql(f"""
    SELECT policy_id, policy_name, policy_type, catalog_name, on_securable_type, to_principals, except_principals
    FROM {catalog}.information_schema.abac_policy_definitions
    WHERE policy_name = 'abac_tdm_protection_policy'
    """)
    display(audit_df)
except Exception as e:
    print(f"Metadata verification note: {e}")