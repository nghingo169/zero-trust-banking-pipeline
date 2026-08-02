-- Compare the exact source-customer references that are unresolved in both
-- KYC and employment. Requires the KYC and employment resolution views.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

WITH unresolved_kyc AS (
  SELECT
    k.customer_ref,
    COUNT(*) AS unresolved_kyc_records
  FROM customer_kyc k
  JOIN party_identity_resolution_candidate_v r
    ON r.source_business_key = k.kyc_id
  WHERE k.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'
  GROUP BY k.customer_ref
),
unresolved_employment AS (
  SELECT
    e.customer_ref,
    COUNT(*) AS unresolved_employment_records
  FROM customer_employment e
  JOIN employment_identity_resolution_candidate_v r
    ON r.source_business_key = e.employment_id
  WHERE e.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'
  GROUP BY e.customer_ref
)
SELECT
  CASE
    WHEN k.customer_ref IS NOT NULL AND e.customer_ref IS NOT NULL
      THEN 'UNRESOLVED_IN_BOTH'
    WHEN k.customer_ref IS NOT NULL
      THEN 'KYC_ONLY'
    ELSE 'EMPLOYMENT_ONLY'
  END AS orphan_population,
  COUNT(*) AS distinct_customer_references,
  SUM(COALESCE(k.unresolved_kyc_records, 0)) AS unresolved_kyc_records,
  SUM(COALESCE(e.unresolved_employment_records, 0))
    AS unresolved_employment_records
FROM unresolved_kyc k
FULL OUTER JOIN unresolved_employment e
  ON e.customer_ref = k.customer_ref
GROUP BY
  CASE
    WHEN k.customer_ref IS NOT NULL AND e.customer_ref IS NOT NULL
      THEN 'UNRESOLVED_IN_BOTH'
    WHEN k.customer_ref IS NOT NULL
      THEN 'KYC_ONLY'
    ELSE 'EMPLOYMENT_ONLY'
  END
ORDER BY orphan_population;


-- Optional: inspect the overlapping orphan references without multiplying
-- KYC and employment records.
WITH unresolved_kyc AS (
  SELECT
    k.customer_ref,
    COUNT(*) AS unresolved_kyc_records
  FROM customer_kyc k
  JOIN party_identity_resolution_candidate_v r
    ON r.source_business_key = k.kyc_id
  WHERE k.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'
  GROUP BY k.customer_ref
),
unresolved_employment AS (
  SELECT
    e.customer_ref,
    COUNT(*) AS unresolved_employment_records
  FROM customer_employment e
  JOIN employment_identity_resolution_candidate_v r
    ON r.source_business_key = e.employment_id
  WHERE e.__END_AT IS NULL
    AND r.resolution_status = 'UNRESOLVED'
  GROUP BY e.customer_ref
)
SELECT
  k.customer_ref,
  k.unresolved_kyc_records,
  e.unresolved_employment_records
FROM unresolved_kyc k
JOIN unresolved_employment e
  ON e.customer_ref = k.customer_ref
ORDER BY k.customer_ref
LIMIT 100;
