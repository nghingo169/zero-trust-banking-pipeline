-- Customer 360 test layer: explain unresolved KYC references.
-- Requires 06_party_identity_resolution_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

-- Break statuses down by the source-key format carried in customer_ref.
WITH active_kyc AS (
  SELECT kyc_id, customer_ref
  FROM customer_kyc
  WHERE __END_AT IS NULL
)
SELECT
  r.resolution_status,
  CASE
    WHEN k.customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
    WHEN k.customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    ELSE 'UNRECOGNISED_FORMAT'
  END AS reference_type,
  COUNT(*) AS kyc_record_count
FROM party_identity_resolution_candidate_v r
JOIN active_kyc k
  ON k.kyc_id = r.source_business_key
GROUP BY
  r.resolution_status,
  CASE
    WHEN k.customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
    WHEN k.customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    ELSE 'UNRECOGNISED_FORMAT'
  END
ORDER BY r.resolution_status, reference_type;


-- For unresolved rows, determine whether the source key is absent from its
-- active clean customer master, or present there but missing from the party
-- identifier view. The latter indicates a mapping bug; the former should be
-- quarantined as an unresolved customer reference.
WITH active_kyc AS (
  SELECT kyc_id, customer_ref
  FROM customer_kyc
  WHERE __END_AT IS NULL
),
unresolved AS (
  SELECT k.kyc_id, k.customer_ref
  FROM active_kyc k
  JOIN party_identity_resolution_candidate_v r
    ON r.source_business_key = k.kyc_id
  WHERE r.resolution_status = 'UNRESOLVED'
)
SELECT
  CASE
    WHEN u.customer_ref LIKE 'CB-%'
     AND c.cust_no IS NOT NULL
      THEN 'PRESENT_IN_CORE_BUT_NOT_MAPPED'
    WHEN u.customer_ref LIKE 'CRM-%'
     AND crm.party_id IS NOT NULL
      THEN 'PRESENT_IN_CRM_BUT_NOT_MAPPED'
    WHEN u.customer_ref LIKE 'CB-%'
      THEN 'MISSING_FROM_ACTIVE_CORE'
    WHEN u.customer_ref LIKE 'CRM-%'
      THEN 'MISSING_FROM_ACTIVE_CRM'
    ELSE 'UNRECOGNISED_FORMAT'
  END AS unresolved_reason,
  COUNT(*) AS kyc_record_count
FROM unresolved u
LEFT JOIN core_customer_active_v c
  ON u.customer_ref = c.cust_no
LEFT JOIN crm_customer_active_v crm
  ON u.customer_ref = crm.party_id
GROUP BY
  CASE
    WHEN u.customer_ref LIKE 'CB-%'
     AND c.cust_no IS NOT NULL
      THEN 'PRESENT_IN_CORE_BUT_NOT_MAPPED'
    WHEN u.customer_ref LIKE 'CRM-%'
     AND crm.party_id IS NOT NULL
      THEN 'PRESENT_IN_CRM_BUT_NOT_MAPPED'
    WHEN u.customer_ref LIKE 'CB-%'
      THEN 'MISSING_FROM_ACTIVE_CORE'
    WHEN u.customer_ref LIKE 'CRM-%'
      THEN 'MISSING_FROM_ACTIVE_CRM'
    ELSE 'UNRECOGNISED_FORMAT'
  END
ORDER BY unresolved_reason;
