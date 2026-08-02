-- Customer 360 test layer: one canonical party candidate per safe identity.
-- Requires 01_active_customer_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW party_candidate_v AS
WITH source_customers AS (
  SELECT
    'CORE_BANKING' AS source_system,
    cust_no AS source_business_key,
    national_id_token,
    business_date,
    __START_AT,
    ingested_at
  FROM core_customer_active_v

  UNION ALL

  SELECT
    'CRM' AS source_system,
    party_id AS source_business_key,
    national_id_token,
    business_date,
    __START_AT,
    ingested_at
  FROM crm_customer_active_v
),
identity_counts AS (
  SELECT
    *,
    COUNT_IF(source_system = 'CORE_BANKING') OVER (
      PARTITION BY national_id_token
    ) AS core_active_count,
    COUNT_IF(source_system = 'CRM') OVER (
      PARTITION BY national_id_token
    ) AS crm_active_count
  FROM source_customers
),
eligible_source_customers AS (
  SELECT *
  FROM identity_counts
  WHERE core_active_count <= 1
    AND crm_active_count <= 1
)
SELECT
  -- Test-only deterministic key; a physical party table will allocate P-keys.
  concat(
    'P-',
    upper(substr(sha2(concat('PARTY_TEST|', national_id_token), 256), 1, 12))
  ) AS party_key,
  'PERSON' AS party_type,
  'ACTIVE' AS party_status,
  'VALID' AS data_quality_status,
  CASE
    WHEN MAX(core_active_count) = 1 AND MAX(crm_active_count) = 1
      THEN 'CORE_AND_CRM'
    WHEN MAX(core_active_count) = 1
      THEN 'CORE_ONLY'
    ELSE 'CRM_ONLY'
  END AS source_coverage,
  MAX(business_date) AS latest_business_date,
  MAX(ingested_at) AS latest_ingested_at
FROM eligible_source_customers
GROUP BY national_id_token;
