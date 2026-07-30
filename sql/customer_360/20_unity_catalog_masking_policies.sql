-- ============================================================================= 
-- Unity Catalog Masking Policies for NAB TDM
-- =============================================================================
-- Purpose: Implement role-based PII unmasking for authorized compliance officers
-- 
-- Architecture:
--   Layer 1: Format-preserving masking at transformation time (in Silver tables)
--   Layer 2: Reversible AES encryption for sensitive fields (stored encrypted)
--   Layer 3: UC masking policies for query-time decryption (authorized users only)
-- 
-- Usage:
--   1. Run this script as admin user to create masking policies
--   2. Apply policies to silver atomic tables
--   3. Grant unmask privilege to compliance officer groups
-- =============================================================================

USE CATALOG workspace;
USE SCHEMA silver;

-- =============================================================================
-- 1. Create Masking Policy for Card Numbers
-- =============================================================================
-- Compliance officers see full card numbers, others see only masked BIN+suffix
CREATE OR REPLACE FUNCTION mask_card_number_policy(card_token STRING)
RETURNS STRING
COMMENT 'NAB TDM: Unmask card numbers for compliance_officers group only. Regular users see BIN + last 4 digits.'
RETURN 
  CASE
    WHEN is_account_group_member('compliance_officers') THEN
      -- Decrypt and show full card number to authorized users
      CAST(aes_decrypt(unbase64(card_token), 'NAB_UC_KEY') AS STRING)
    ELSE
      -- Show only masked version (BIN + last 4) to regular users
      CONCAT('XXXX-XXXX-', SUBSTRING(card_token, -4, 4))
  END;

-- =============================================================================
-- 2. Create Masking Policy for National IDs
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_national_id_policy(national_id_token STRING)
RETURNS STRING
COMMENT 'NAB TDM: Unmask national IDs for compliance_officers and kyc_analysts. Regular users see format-preserving masked value.'
RETURN 
  CASE
    WHEN is_account_group_member('compliance_officers') OR 
         is_account_group_member('kyc_analysts') THEN
      -- Decrypt for authorized KYC/compliance teams
      CAST(aes_decrypt(unbase64(national_id_token), 'NAB_UC_KEY') AS STRING)
    ELSE
      -- Show only format-preserving masked value
      national_id_token
  END;

-- =============================================================================
-- 3. Create Masking Policy for Phone Numbers
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_phone_policy(phone_masked STRING)
RETURNS STRING
COMMENT 'NAB TDM: Unmask phones for compliance_officers and customer_service. Regular users see area code + XXXX + last 4.'
RETURN 
  CASE
    WHEN is_account_group_member('compliance_officers') OR 
         is_account_group_member('customer_service') THEN
      -- Decrypt for customer service and compliance
      CAST(aes_decrypt(unbase64(phone_masked), 'NAB_UC_KEY') AS STRING)
    ELSE
      -- Show area code + masked middle/last digits
      CONCAT(SUBSTRING(phone_masked, 1, 4), '-XXXX-', SUBSTRING(phone_masked, -4, 4))
  END;

-- =============================================================================
-- 4. Create Masking Policy for Email Addresses
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_email_policy(email_masked STRING)
RETURNS STRING
COMMENT 'NAB TDM: Unmask email for compliance_officers only. Format: customer_number@notreal.nab.com.au for others.'
RETURN 
  CASE
    WHEN is_account_group_member('compliance_officers') THEN
      -- Decrypt for compliance
      CAST(aes_decrypt(unbase64(email_masked), 'NAB_UC_KEY') AS STRING)
    ELSE
      -- Show masked email per NAB TDM standard
      email_masked
  END;

-- =============================================================================
-- 5. Apply Column Masks to Silver Atomic Tables
-- =============================================================================

-- Apply to payment_card table
ALTER TABLE workspace.silver.payment_card 
  ALTER COLUMN card_number_token 
  SET MASK mask_card_number_policy;

COMMENT ON TABLE workspace.silver.payment_card IS 
  'Canonical Silver Payment Card Table with UC masking policies applied.';

-- Apply to party table (assumes party table has these fields)
ALTER TABLE workspace.silver.party
ALTER COLUMN national_id_token SET MASK mask_national_id_policy;

ALTER TABLE workspace.silver.party
ALTER COLUMN phone_masked SET MASK mask_phone_policy;

ALTER TABLE workspace.silver.party
ALTER COLUMN email_masked SET MASK mask_email_policy;

-- =============================================================================
-- 6. Grant Unmask Privileges to Compliance Group
-- =============================================================================

-- Create compliance officers group (if not exists)
CREATE GROUP IF NOT EXISTS compliance_officers;

-- Grant SELECT with unmask capability
GRANT SELECT ON TABLE workspace.silver.payment_card TO `compliance_officers`;
GRANT USE SCHEMA ON SCHEMA workspace.silver TO `compliance_officers`;
GRANT USE CATALOG ON CATALOG workspace TO `compliance_officers`;

-- Verify masking policies are applied
SHOW MASKS ON TABLE workspace.silver.payment_card;

-- =============================================================================
-- 7. Usage Examples
-- =============================================================================

-- As regular user (AI agent, analyst):
--   SELECT card_number_token FROM workspace.silver.payment_card;
--   Result: XXXX-XXXX-1234 (masked)

-- As compliance officer:
--   SELECT card_number_token FROM workspace.silver.payment_card;
--   Result: 4532-1234-5678-9010 (full card number)

-- =============================================================================
-- 8. Audit Logging for Unmask Access
-- =============================================================================

-- Enable audit logging for unmask events (Databricks Premium/Enterprise only)
ALTER TABLE workspace.silver.payment_card 
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Query audit logs to track who unmasked what
SELECT 
  event_date,
  user_identity.email as user_email,
  request_params.table_name,
  request_params.column_name,
  action_name
FROM system.access.audit
WHERE action_name = 'columnMaskUnmasked'
  AND request_params.table_name = 'workspace.silver.payment_card'
ORDER BY event_date DESC
LIMIT 100;

-- =============================================================================
-- END OF UC MASKING POLICIES
-- =============================================================================
