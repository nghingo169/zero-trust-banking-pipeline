-- Customer 360 test layer: employment resolution and reconciliation checks.
-- Requires 11_party_employment_resolution_and_quarantine_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

SELECT
  (SELECT COUNT(*) FROM customer_employment WHERE __END_AT IS NULL)
    AS active_employment_rows,
  (SELECT COUNT(*) FROM party_employment_candidate_v) AS resolved_employment_rows,
  (SELECT COUNT(*) FROM customer_employment_unresolved_reference_candidate_v)
    AS quarantined_employment_rows,
  (SELECT COUNT(*) FROM party_employment_candidate_v)
    + (SELECT COUNT(*) FROM customer_employment_unresolved_reference_candidate_v)
    AS reconciled_employment_rows;


SELECT
  resolution_status,
  COUNT(*) AS resolution_count
FROM employment_identity_resolution_candidate_v
GROUP BY resolution_status
ORDER BY resolution_status;


-- Expected: zero rows. Curated employment must have a resolved party.
SELECT *
FROM party_employment_candidate_v
WHERE party_key IS NULL;


-- Expected: zero rows. One current employment output per source record.
SELECT
  source_business_key,
  COUNT(*) AS employment_rows
FROM party_employment_candidate_v
GROUP BY source_business_key
HAVING COUNT(*) <> 1;
