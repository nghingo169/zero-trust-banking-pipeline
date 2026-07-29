-- Customer 360 test layer: KYC identity-resolution quality checks.
-- Requires 06_party_identity_resolution_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

-- Expected: one output row for every active KYC record.
SELECT
  (SELECT COUNT(*) FROM customer_kyc WHERE __END_AT IS NULL) AS active_kyc_rows,
  (SELECT COUNT(*) FROM party_identity_resolution_candidate_v)
    AS identity_resolution_rows;


-- Expected: zero rows. A resolution record has exactly one row per KYC key.
SELECT
  source_system,
  source_entity,
  source_business_key,
  COUNT(*) AS resolution_rows
FROM party_identity_resolution_candidate_v
GROUP BY source_system, source_entity, source_business_key
HAVING COUNT(*) <> 1;


-- Inspect the confirmed / unresolved / ambiguous distribution.
SELECT
  resolution_status,
  COUNT(*) AS resolution_count
FROM party_identity_resolution_candidate_v
GROUP BY resolution_status
ORDER BY resolution_status;


-- Expected: zero rows. Only confirmed records receive a party key.
SELECT
  identity_resolution_key,
  resolution_status,
  candidate_party_key
FROM party_identity_resolution_candidate_v
WHERE (resolution_status = 'CONFIRMED' AND candidate_party_key IS NULL)
   OR (resolution_status <> 'CONFIRMED' AND candidate_party_key IS NOT NULL);
