-- Customer 360 test layer: explain unresolved employment references.
-- Requires 11_party_employment_resolution_and_quarantine_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

WITH active_employment AS (
  SELECT employment_id, customer_ref
  FROM customer_employment
  WHERE __END_AT IS NULL
)
SELECT
  r.resolution_status,
  CASE
    WHEN e.customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
    WHEN e.customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    ELSE 'UNRECOGNISED_FORMAT'
  END AS reference_type,
  COUNT(*) AS employment_record_count
FROM employment_identity_resolution_candidate_v r
JOIN active_employment e
  ON e.employment_id = r.source_business_key
GROUP BY
  r.resolution_status,
  CASE
    WHEN e.customer_ref LIKE 'CB-%' THEN 'CORE_CUST_NO'
    WHEN e.customer_ref LIKE 'CRM-%' THEN 'CRM_PARTY_ID'
    ELSE 'UNRECOGNISED_FORMAT'
  END
ORDER BY r.resolution_status, reference_type;


WITH active_employment AS (
  SELECT employment_id, customer_ref
  FROM customer_employment
  WHERE __END_AT IS NULL
),
unresolved AS (
  SELECT e.employment_id, e.customer_ref
  FROM active_employment e
  JOIN employment_identity_resolution_candidate_v r
    ON r.source_business_key = e.employment_id
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
  COUNT(*) AS employment_record_count
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
