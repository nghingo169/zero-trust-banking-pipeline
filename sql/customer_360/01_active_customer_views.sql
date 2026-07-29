-- Customer 360 test layer: active validated customer records.
USE CATALOG workspace;
USE SCHEMA team_customer_silver;

CREATE OR REPLACE VIEW core_customer_active_v AS
SELECT
  cust_no,
  sha2(concat('CUSTOMER_360|', upper(trim(national_id))), 256)
    AS national_id_token,
  business_date,
  __START_AT,
  __END_AT,
  EXTRACT_DTE AS ingested_at
FROM core_banking_customer
WHERE __END_AT IS NULL
  AND national_id IS NOT NULL
  AND trim(national_id) <> '';


CREATE OR REPLACE VIEW crm_customer_active_v AS
SELECT
  party_id,
  sha2(concat('CUSTOMER_360|', upper(trim(national_id))), 256)
    AS national_id_token,
  business_date,
  __START_AT,
  __END_AT,
  EXTRACT_DTE AS ingested_at
FROM crm_customer
WHERE __END_AT IS NULL
  AND national_id IS NOT NULL
  AND trim(national_id) <> '';
