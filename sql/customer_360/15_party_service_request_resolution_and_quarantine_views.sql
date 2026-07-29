-- Customer 360 test layer: resolve active service requests to canonical
-- parties and retain orphan references as quarantine evidence.
-- Requires 04_party_identifier_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW request_identity_resolution_candidate_v AS
WITH active_requests AS (
  SELECT
    request_id,
    customer_ref,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_request
  WHERE __END_AT IS NULL
),
typed_requests AS (
  SELECT
    *,
    CASE
      WHEN customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
      WHEN customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    END AS reference_identifier_type
  FROM active_requests
),
candidate_matches AS (
  SELECT
    q.request_id,
    q.business_date,
    q.ingested_at,
    i.party_key AS matched_party_key,
    COUNT(i.party_key) OVER (PARTITION BY q.request_id) AS candidate_party_count,
    ROW_NUMBER() OVER (
      PARTITION BY q.request_id
      ORDER BY i.party_key NULLS LAST
    ) AS match_row_number
  FROM typed_requests q
  LEFT JOIN party_identifier_candidate_v i
    ON i.identifier_type = q.reference_identifier_type
   AND i.identifier_value_token = sha2(
     concat('IDENTIFIER|', q.customer_ref),
     256
   )
   AND i.valid_to IS NULL
)
SELECT
  concat('IR-', upper(substr(
    sha2(concat('REQUEST|', request_id), 256), 1, 16
  ))) AS identity_resolution_key,
  'SERVICE_REQUEST' AS source_system,
  'customer_request' AS source_entity,
  request_id AS source_business_key,
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
    'customer_request|request_id=', request_id,
    '|business_date=', cast(business_date AS STRING)
  ) AS bronze_record_ref,
  ingested_at
FROM candidate_matches
WHERE match_row_number = 1;


CREATE OR REPLACE VIEW party_service_request_candidate_v AS
WITH active_requests AS (
  SELECT
    request_id,
    request_type,
    channel,
    request_date,
    status,
    resolution_date,
    description,
    business_date,
    EXTRACT_DTE AS ingested_at
  FROM customer_request
  WHERE __END_AT IS NULL
)
SELECT
  concat('SR-', upper(substr(
    sha2(concat('REQUEST|', q.request_id), 256), 1, 16
  ))) AS service_request_key,
  r.candidate_party_key AS party_key,
  q.request_type,
  q.channel,
  q.request_date,
  q.status AS request_status,
  q.resolution_date,
  q.description AS request_description,
  'SERVICE_REQUEST' AS source_system,
  q.request_id AS source_business_key,
  concat(
    'customer_request|request_id=', q.request_id,
    '|business_date=', cast(q.business_date AS STRING)
  ) AS bronze_record_ref,
  q.ingested_at
FROM active_requests q
JOIN request_identity_resolution_candidate_v r
  ON r.source_business_key = q.request_id
WHERE r.resolution_status = 'CONFIRMED';


CREATE OR REPLACE VIEW customer_request_unresolved_reference_candidate_v AS
SELECT
  r.source_business_key AS request_id,
  r.resolution_status,
  CASE
    WHEN r.resolution_status = 'UNRESOLVED'
      THEN 'UNRESOLVED_CUSTOMER_REFERENCE'
    ELSE 'AMBIGUOUS_CUSTOMER_REFERENCE'
  END AS quarantine_reason,
  r.bronze_record_ref,
  r.ingested_at AS quarantined_at
FROM request_identity_resolution_candidate_v r
WHERE r.resolution_status <> 'CONFIRMED';
