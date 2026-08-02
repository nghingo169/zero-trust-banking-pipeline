-- Customer 360 test layer: source identifiers mapped to canonical parties.
-- Requires 01_active_customer_views.sql and 02_party_candidate_view.sql.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW party_identifier_candidate_v AS
WITH core_identifiers AS (
  SELECT
    c.national_id_token,
    'CORE_CUST_NO' AS identifier_type,
    c.cust_no AS identifier_value,
    'CORE_BANKING' AS source_system,
    TRUE AS is_primary,
    c.__START_AT AS valid_from,
    c.__END_AT AS valid_to,
    c.cust_no AS source_business_key,
    concat(
      'core_banking_customer|cust_no=', c.cust_no,
      '|business_date=', cast(c.business_date AS STRING)
    ) AS bronze_record_ref,
    c.ingested_at
  FROM core_customer_active_v c
),
crm_identifiers AS (
  SELECT
    c.national_id_token,
    'CRM_PARTY_ID' AS identifier_type,
    c.party_id AS identifier_value,
    'CRM' AS source_system,
    TRUE AS is_primary,
    c.__START_AT AS valid_from,
    c.__END_AT AS valid_to,
    c.party_id AS source_business_key,
    concat(
      'crm_customer|party_id=', c.party_id,
      '|business_date=', cast(c.business_date AS STRING)
    ) AS bronze_record_ref,
    c.ingested_at
  FROM crm_customer_active_v c
),
all_identifiers AS (
  SELECT * FROM core_identifiers
  UNION ALL
  SELECT * FROM crm_identifiers
)
SELECT
  concat(
    'PI-',
    upper(substr(
      sha2(concat(
        p.party_key, '|', i.source_system, '|', i.identifier_type, '|',
        i.identifier_value
      ), 256),
      1,
      16
    ))
  ) AS party_identifier_key,
  p.party_key,
  i.identifier_type,
  sha2(concat('IDENTIFIER|', i.identifier_value), 256)
    AS identifier_value_token,
  i.source_system,
  i.is_primary,
  i.valid_from,
  i.valid_to,
  i.source_business_key,
  i.bronze_record_ref,
  i.ingested_at
FROM all_identifiers i
JOIN party_candidate_v p
  ON p.party_key = concat(
    'P-',
    upper(substr(sha2(concat('PARTY_TEST|', i.national_id_token), 256), 1, 12))
  );
