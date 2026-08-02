-- Summarize unique orphan customer references across KYC, employment, and
-- service requests. Requires the three identity-resolution candidate views.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

WITH orphan_records AS (
  SELECT
    k.customer_ref,
    'KYC' AS source_entity
  FROM customer_kyc k
  JOIN party_identity_resolution_candidate_v r
    ON r.source_business_key = k.kyc_id
  WHERE k.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'

  UNION ALL

  SELECT
    e.customer_ref,
    'EMPLOYMENT' AS source_entity
  FROM customer_employment e
  JOIN employment_identity_resolution_candidate_v r
    ON r.source_business_key = e.employment_id
  WHERE e.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'

  UNION ALL

  SELECT
    q.customer_ref,
    'SERVICE_REQUEST' AS source_entity
  FROM customer_request q
  JOIN request_identity_resolution_candidate_v r
    ON r.source_business_key = q.request_id
  WHERE q.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'
),
per_customer_reference AS (
  SELECT
    customer_ref,
    COUNT_IF(source_entity = 'KYC') AS kyc_orphan_records,
    COUNT_IF(source_entity = 'EMPLOYMENT') AS employment_orphan_records,
    COUNT_IF(source_entity = 'SERVICE_REQUEST') AS request_orphan_records
  FROM orphan_records
  GROUP BY customer_ref
),
classified AS (
  SELECT
    *,
    CASE
      WHEN kyc_orphan_records > 0
       AND employment_orphan_records > 0
       AND request_orphan_records > 0
        THEN 'ALL_THREE'
      WHEN kyc_orphan_records > 0 AND employment_orphan_records > 0
        THEN 'KYC_AND_EMPLOYMENT'
      WHEN kyc_orphan_records > 0 AND request_orphan_records > 0
        THEN 'KYC_AND_SERVICE_REQUEST'
      WHEN employment_orphan_records > 0 AND request_orphan_records > 0
        THEN 'EMPLOYMENT_AND_SERVICE_REQUEST'
      WHEN kyc_orphan_records > 0
        THEN 'KYC_ONLY'
      WHEN employment_orphan_records > 0
        THEN 'EMPLOYMENT_ONLY'
      ELSE 'SERVICE_REQUEST_ONLY'
    END AS orphan_population
  FROM per_customer_reference
),
summary AS (
  SELECT
    orphan_population,
    COUNT(*) AS distinct_orphan_customer_references,
    SUM(kyc_orphan_records) AS kyc_orphan_records,
    SUM(employment_orphan_records) AS employment_orphan_records,
    SUM(request_orphan_records) AS request_orphan_records,
    SUM(
      kyc_orphan_records + employment_orphan_records + request_orphan_records
    ) AS total_orphan_records
  FROM classified
  GROUP BY orphan_population
)
SELECT
  'ALL_ORPHAN_CUSTOMER_REFERENCES' AS orphan_population,
  SUM(distinct_orphan_customer_references) AS distinct_orphan_customer_references,
  SUM(kyc_orphan_records) AS kyc_orphan_records,
  SUM(employment_orphan_records) AS employment_orphan_records,
  SUM(request_orphan_records) AS request_orphan_records,
  SUM(total_orphan_records) AS total_orphan_records
FROM summary

UNION ALL

SELECT
  orphan_population,
  distinct_orphan_customer_references,
  kyc_orphan_records,
  employment_orphan_records,
  request_orphan_records,
  total_orphan_records
FROM summary
ORDER BY orphan_population;
