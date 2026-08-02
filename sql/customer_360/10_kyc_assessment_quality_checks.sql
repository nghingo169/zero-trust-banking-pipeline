-- Customer 360 test layer: KYC publication / quarantine reconciliation.
-- Requires 09_party_kyc_assessment_and_quarantine_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

-- Expected: resolved KYC + quarantined KYC = all active KYC records.
SELECT
  (SELECT COUNT(*) FROM customer_kyc WHERE __END_AT IS NULL) AS active_kyc_rows,
  (SELECT COUNT(*) FROM party_kyc_assessment_candidate_v) AS resolved_kyc_rows,
  (SELECT COUNT(*) FROM customer_kyc_unresolved_reference_candidate_v)
    AS quarantined_kyc_rows,
  (SELECT COUNT(*) FROM party_kyc_assessment_candidate_v)
    + (SELECT COUNT(*) FROM customer_kyc_unresolved_reference_candidate_v)
    AS reconciled_kyc_rows;


-- Expected: zero rows. Curated KYC must always have a resolved party.
SELECT *
FROM party_kyc_assessment_candidate_v
WHERE party_key IS NULL;


-- Expected: one row for UNRESOLVED_CUSTOMER_REFERENCE with 8,050 rows.
SELECT
  quarantine_reason,
  COUNT(*) AS quarantined_kyc_rows
FROM customer_kyc_unresolved_reference_candidate_v
GROUP BY quarantine_reason;
