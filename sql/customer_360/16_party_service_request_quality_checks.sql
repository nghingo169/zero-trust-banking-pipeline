-- Customer 360 test layer: service-request resolution and reconciliation.
-- Requires 15_party_service_request_resolution_and_quarantine_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

SELECT
  (SELECT COUNT(*) FROM customer_request WHERE __END_AT IS NULL)
    AS active_request_rows,
  (SELECT COUNT(*) FROM party_service_request_candidate_v)
    AS resolved_request_rows,
  (SELECT COUNT(*) FROM customer_request_unresolved_reference_candidate_v)
    AS quarantined_request_rows,
  (SELECT COUNT(*) FROM party_service_request_candidate_v)
    + (SELECT COUNT(*) FROM customer_request_unresolved_reference_candidate_v)
    AS reconciled_request_rows;


SELECT
  resolution_status,
  COUNT(*) AS resolution_count
FROM request_identity_resolution_candidate_v
GROUP BY resolution_status
ORDER BY resolution_status;


-- Expected: zero rows. Curated requests must have a resolved party.
SELECT *
FROM party_service_request_candidate_v
WHERE party_key IS NULL;


-- Expected: zero rows. One current request output per source request.
SELECT
  source_business_key,
  COUNT(*) AS request_rows
FROM party_service_request_candidate_v
GROUP BY source_business_key
HAVING COUNT(*) <> 1;
