-- Customer 360 test layer: repeatable quality checks.
-- Requires 01_active_customer_views.sql and 02_party_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

-- Expected: zero rows. Core duplicate active identities after normalization.
SELECT
  'CORE_BANKING' AS source_system,
  national_id_token,
  COUNT(*) AS active_row_count
FROM core_customer_active_v
GROUP BY national_id_token
HAVING COUNT(*) > 1;


-- Expected: zero rows. CRM duplicate active identities after normalization.
SELECT
  'CRM' AS source_system,
  national_id_token,
  COUNT(*) AS active_row_count
FROM crm_customer_active_v
GROUP BY national_id_token
HAVING COUNT(*) > 1;


-- Expected: one row per coverage type; the full canonical-party count.
SELECT
  source_coverage,
  COUNT(*) AS party_count
FROM party_candidate_v
GROUP BY source_coverage
ORDER BY source_coverage;


-- Expected: zero rows. One candidate row per canonical party key.
SELECT
  party_key,
  COUNT(*) AS row_count
FROM party_candidate_v
GROUP BY party_key
HAVING COUNT(*) > 1;


-- Expected: candidate count = active Core + active CRM - Core/CRM overlap.
SELECT
  (SELECT COUNT(*) FROM core_customer_active_v) AS core_active_rows,
  (SELECT COUNT(*) FROM crm_customer_active_v) AS crm_active_rows,
  (SELECT COUNT(*) FROM core_customer_active_v)
    + (SELECT COUNT(*) FROM crm_customer_active_v) AS total_active_source_rows,
  (SELECT COUNT(*) FROM party_candidate_v) AS canonical_party_count;
