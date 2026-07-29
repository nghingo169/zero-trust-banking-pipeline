-- Customer 360 test layer: identifier quality checks.
-- Requires 04_party_identifier_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

-- Expected: zero rows. An active identifier has one mapping in its source.
SELECT
  source_system,
  identifier_type,
  identifier_value_token,
  COUNT(*) AS identifier_mappings
FROM party_identifier_candidate_v
GROUP BY source_system, identifier_type, identifier_value_token
HAVING COUNT(*) <> 1;


-- Expected: zero rows. Every identifier resolves to exactly one party.
SELECT
  party_identifier_key,
  COUNT(DISTINCT party_key) AS party_count
FROM party_identifier_candidate_v
GROUP BY party_identifier_key
HAVING COUNT(DISTINCT party_key) <> 1;


-- Expected: one row per type/source combination.
SELECT
  source_system,
  identifier_type,
  is_primary,
  COUNT(*) AS identifier_count
FROM party_identifier_candidate_v
GROUP BY source_system, identifier_type, is_primary
ORDER BY source_system, identifier_type;
