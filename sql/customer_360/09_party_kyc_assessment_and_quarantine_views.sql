-- Customer 360 test layer: publish resolved KYC assessments and preserve
-- unresolved KYC references as quarantine evidence.
-- Requires 06_party_identity_resolution_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW party_kyc_assessment_candidate_v AS
WITH active_kyc AS (
  SELECT
    kyc_id,
    id_type,
    id_number,
    verification_status,
    verified_date,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_kyc
  WHERE __END_AT IS NULL
)
SELECT
  concat('KYC-', upper(substr(sha2(concat('KYC|', k.kyc_id), 256), 1, 16)))
    AS kyc_assessment_key,
  r.candidate_party_key AS party_key,
  k.id_type,
  sha2(concat('ID_NUMBER|', k.id_number), 256) AS id_number_token,
  k.verification_status,
  k.verified_date,
  'KYC' AS source_system,
  k.kyc_id AS source_business_key,
  concat(
    'customer_kyc|kyc_id=', k.kyc_id,
    '|business_date=', cast(k.business_date AS STRING)
  ) AS bronze_record_ref,
  k.ingested_at
FROM active_kyc k
JOIN party_identity_resolution_candidate_v r
  ON r.source_business_key = k.kyc_id
WHERE r.resolution_status = 'CONFIRMED';


CREATE OR REPLACE VIEW customer_kyc_unresolved_reference_candidate_v AS
WITH active_kyc AS (
  SELECT
    kyc_id,
    customer_ref,
    id_type,
    verification_status,
    verified_date,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_kyc
  WHERE __END_AT IS NULL
)
SELECT
  k.business_date AS as_of_date,
  k.kyc_id,
  k.customer_ref,
  k.id_type,
  k.verification_status,
  k.verified_date,
  CASE
    WHEN r.resolution_status = 'UNRESOLVED'
      THEN 'UNRESOLVED_CUSTOMER_REFERENCE'
    ELSE 'AMBIGUOUS_CUSTOMER_REFERENCE'
  END AS quarantine_reason,
  k.ingested_at AS quarantined_at
FROM active_kyc k
JOIN party_identity_resolution_candidate_v r
  ON r.source_business_key = k.kyc_id
WHERE r.resolution_status <> 'CONFIRMED';
