-- Customer 360 test layer: explain unresolved service-request references.
-- Requires 15_party_service_request_resolution_and_quarantine_views.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

WITH active_requests AS (
  SELECT request_id, customer_ref
  FROM customer_request
  WHERE __END_AT IS NULL
),
unresolved AS (
  SELECT q.request_id, q.customer_ref
  FROM active_requests q
  JOIN request_identity_resolution_candidate_v r
    ON r.source_business_key = q.request_id
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
  COUNT(*) AS request_record_count
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
