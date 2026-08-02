-- Customer 360 test layer: resolve active KYC records to canonical parties.
-- Requires 04_party_identifier_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW party_identity_resolution_candidate_v AS
WITH active_kyc AS (
  SELECT
    kyc_id,
    customer_ref,
    business_date,
    __START_AT,
    __END_AT,
    EXTRACT_DTE AS ingested_at
  FROM customer_kyc
  WHERE __END_AT IS NULL
),
typed_kyc AS (
  SELECT
    *,
    CASE
      WHEN customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
      WHEN customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    END AS reference_identifier_type
  FROM active_kyc
),
candidate_matches AS (
  SELECT
    k.kyc_id,
    k.customer_ref,
    k.business_date,
    k.ingested_at,
    i.party_key AS matched_party_key,
    COUNT(i.party_key) OVER (PARTITION BY k.kyc_id) AS candidate_party_count,
    ROW_NUMBER() OVER (
      PARTITION BY k.kyc_id
      ORDER BY i.party_key NULLS LAST
    ) AS match_row_number
  FROM typed_kyc k
  LEFT JOIN party_identifier_candidate_v i
    ON i.identifier_type = k.reference_identifier_type
   AND i.identifier_value_token = sha2(
     concat('IDENTIFIER|', k.customer_ref),
     256
   )
   AND i.valid_to IS NULL
)
SELECT
  concat('IR-', upper(substr(sha2(concat('KYC|', kyc_id), 256), 1, 16)))
    AS identity_resolution_key,
  'KYC' AS source_system,
  'customer_kyc' AS source_entity,
  kyc_id AS source_business_key,
  CASE
    WHEN candidate_party_count = 1 THEN matched_party_key
  END AS candidate_party_key,
  CASE
    WHEN candidate_party_count = 1 THEN 'CONFIRMED'
    WHEN candidate_party_count = 0 THEN 'UNRESOLVED'
    ELSE 'AMBIGUOUS'
  END AS resolution_status,
  'DIRECT_IDENTIFIER' AS match_method,
  CASE
    WHEN candidate_party_count = 1 THEN CAST(1.0000 AS DECIMAL(5,4))
  END AS match_confidence,
  CASE
    WHEN candidate_party_count = 1 THEN current_timestamp()
  END AS resolved_at,
  concat(
    'customer_kyc|kyc_id=', kyc_id,
    '|business_date=', cast(business_date AS STRING)
  ) AS bronze_record_ref,
  ingested_at
FROM candidate_matches
WHERE match_row_number = 1;
