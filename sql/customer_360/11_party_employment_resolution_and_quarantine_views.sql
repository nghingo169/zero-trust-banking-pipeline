-- Customer 360 test layer: resolve active employment records to canonical
-- parties and retain orphan references as quarantine evidence.
-- Requires 04_party_identifier_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW employment_identity_resolution_candidate_v AS
WITH active_employment AS (
  SELECT
    employment_id,
    customer_ref,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_employment
  WHERE __END_AT IS NULL
),
typed_employment AS (
  SELECT
    *,
    CASE
      WHEN customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
      WHEN customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    END AS reference_identifier_type
  FROM active_employment
),
candidate_matches AS (
  SELECT
    e.employment_id,
    e.business_date,
    e.ingested_at,
    i.party_key AS matched_party_key,
    COUNT(i.party_key) OVER (PARTITION BY e.employment_id) AS candidate_party_count,
    ROW_NUMBER() OVER (
      PARTITION BY e.employment_id
      ORDER BY i.party_key NULLS LAST
    ) AS match_row_number
  FROM typed_employment e
  LEFT JOIN party_identifier_candidate_v i
    ON i.identifier_type = e.reference_identifier_type
   AND i.identifier_value_token = sha2(
     concat('IDENTIFIER|', e.customer_ref),
     256
   )
   AND i.valid_to IS NULL
)
SELECT
  concat('IR-', upper(substr(
    sha2(concat('EMPLOYMENT|', employment_id), 256), 1, 16
  ))) AS identity_resolution_key,
  'EMPLOYMENT' AS source_system,
  'customer_employment' AS source_entity,
  employment_id AS source_business_key,
  CASE WHEN candidate_party_count = 1 THEN matched_party_key END
    AS candidate_party_key,
  CASE
    WHEN candidate_party_count = 1 THEN 'CONFIRMED'
    WHEN candidate_party_count = 0 THEN 'UNRESOLVED'
    ELSE 'AMBIGUOUS'
  END AS resolution_status,
  'DIRECT_IDENTIFIER' AS match_method,
  CASE WHEN candidate_party_count = 1 THEN CAST(1.0000 AS DECIMAL(5,4)) END
    AS match_confidence,
  CASE WHEN candidate_party_count = 1 THEN current_timestamp() END AS resolved_at,
  concat(
    'customer_employment|employment_id=', employment_id,
    '|business_date=', cast(business_date AS STRING)
  ) AS bronze_record_ref,
  ingested_at
FROM candidate_matches
WHERE match_row_number = 1;


CREATE OR REPLACE VIEW party_employment_candidate_v AS
WITH active_employment AS (
  SELECT
    employment_id,
    employer_name,
    job_title,
    monthly_income,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_employment
  WHERE __END_AT IS NULL
)
SELECT
  concat('EMP-', upper(substr(
    sha2(concat('EMPLOYMENT|', e.employment_id), 256), 1, 16
  ))) AS employment_key,
  r.candidate_party_key AS party_key,
  e.employer_name,
  e.job_title,
  e.monthly_income,
  e.business_date AS effective_from,
  CAST(NULL AS DATE) AS effective_to,
  'EMPLOYMENT' AS source_system,
  e.employment_id AS source_business_key,
  concat(
    'customer_employment|employment_id=', e.employment_id,
    '|business_date=', cast(e.business_date AS STRING)
  ) AS bronze_record_ref,
  e.ingested_at
FROM active_employment e
JOIN employment_identity_resolution_candidate_v r
  ON r.source_business_key = e.employment_id
WHERE r.resolution_status = 'CONFIRMED';


CREATE OR REPLACE VIEW customer_employment_unresolved_reference_candidate_v AS
SELECT
  r.source_business_key AS employment_id,
  r.resolution_status,
  CASE
    WHEN r.resolution_status = 'UNRESOLVED'
      THEN 'UNRESOLVED_CUSTOMER_REFERENCE'
    ELSE 'AMBIGUOUS_CUSTOMER_REFERENCE'
  END AS quarantine_reason,
  r.bronze_record_ref,
  r.ingested_at AS quarantined_at
FROM employment_identity_resolution_candidate_v r
WHERE r.resolution_status <> 'CONFIRMED';
