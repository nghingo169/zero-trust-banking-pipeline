-- NAB Test Data Management (TDM) Reference Mapping Tables Setup
-- Purpose: Cross-system referential integrity for masked PII
-- Author: NAB Data Engineering Team
-- Version: 1.0

-- ==============================================================================
-- CATALOG & SCHEMA SETUP
-- ==============================================================================

CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.tdm_reference
  COMMENT 'NAB TDM Reference Mapping Tables for Cross-System Consistency';

USE CATALOG workspace;
USE SCHEMA tdm_reference;

-- ==============================================================================
-- 1. INDIVIDUAL NAME MAPPING TABLE
-- ==============================================================================

CREATE OR REPLACE TABLE name_mapping_individual (
  party_key STRING NOT NULL COMMENT 'Surrogate key from Party dimension',
  first_name STRING COMMENT 'Masked first name (from reference pool)',
  middle_name STRING COMMENT 'Masked middle name (from reference pool)',
  last_name STRING COMMENT 'Masked last name (from reference pool)',
  full_name STRING GENERATED ALWAYS AS (CONCAT_WS(' ', first_name, middle_name, last_name)) COMMENT 'Generated full name',
  name_hash STRING COMMENT 'Hash of original name for deterministic lookup',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_name_individual PRIMARY KEY (party_key)
)
CLUSTER BY (party_key)
COMMENT 'NAB TDM Rule 1.2: Individual name masking mapping table'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'tdm.maskingRule' = '1.2',
  'tdm.version' = '2024-10-17'
);

-- Sample reference data pool (expand as needed)
INSERT INTO name_mapping_individual (party_key, first_name, middle_name, last_name, name_hash) VALUES
  ('sample_key_001', 'John', 'Robert', 'Smith', 'hash_001'),
  ('sample_key_002', 'Jane', 'Marie', 'Johnson', 'hash_002'),
  ('sample_key_003', 'Michael', 'James', 'Williams', 'hash_003'),
  ('sample_key_004', 'Sarah', 'Elizabeth', 'Brown', 'hash_004'),
  ('sample_key_005', 'David', 'Thomas', 'Jones', 'hash_005');

-- ==============================================================================
-- 2. ORGANIZATION NAME MAPPING TABLE
-- ==============================================================================

CREATE OR REPLACE TABLE name_mapping_organization (
  party_key STRING NOT NULL COMMENT 'Surrogate key from Party dimension',
  organization_name STRING COMMENT 'Masked organization name',
  trading_name STRING COMMENT 'Masked trading name',
  name_hash STRING COMMENT 'Hash of original name for deterministic lookup',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_name_org PRIMARY KEY (party_key)
)
CLUSTER BY (party_key)
COMMENT 'NAB TDM Rule 1.3: Organization name masking mapping table'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'tdm.maskingRule' = '1.3',
  'tdm.version' = '2024-10-17'
);

-- Sample reference data pool
INSERT INTO name_mapping_organization (party_key, organization_name, trading_name, name_hash) VALUES
  ('org_key_001', 'ABC Holdings Pty Ltd', 'ABC Corp', 'org_hash_001'),
  ('org_key_002', 'XYZ Industries Ltd', 'XYZ Group', 'org_hash_002'),
  ('org_key_003', 'Global Services Inc', 'Global Tech', 'org_hash_003');

-- ==============================================================================
-- 3. ADDRESS MAPPING TABLE
-- ==============================================================================

CREATE OR REPLACE TABLE address_mapping (
  party_key STRING NOT NULL COMMENT 'Surrogate key from Party dimension',
  street_number STRING COMMENT 'Masked street number',
  street_name STRING COMMENT 'Masked street name',
  property_name STRING COMMENT 'Masked property name',
  unit_number STRING COMMENT 'Masked unit number',
  -- Note: city, state, postcode, country are NOT masked per NAB TDM Rule 1.11
  address_hash STRING COMMENT 'Hash of original address for deterministic lookup',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_address PRIMARY KEY (party_key)
)
CLUSTER BY (party_key)
COMMENT 'NAB TDM Rule 1.11: Address masking (street only, retain city/state/postcode)'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'tdm.maskingRule' = '1.11',
  'tdm.version' = '2024-10-17'
);

-- Sample reference data pool
INSERT INTO address_mapping (party_key, street_number, street_name, property_name, unit_number, address_hash) VALUES
  ('party_001', '123', 'Main Street', NULL, 'Unit 1', 'addr_hash_001'),
  ('party_002', '456', 'King Road', 'Plaza Tower', 'Suite 10', 'addr_hash_002'),
  ('party_003', '789', 'Queen Avenue', NULL, 'Apt 5B', 'addr_hash_003');

-- ==============================================================================
-- 4. IDENTIFIER MAPPING TABLE (for cross-system consistency)
-- ==============================================================================

CREATE OR REPLACE TABLE identifier_mapping (
  identifier_type STRING NOT NULL COMMENT 'Type: NATIONAL_ID, PHONE, EMAIL, etc.',
  original_hash STRING NOT NULL COMMENT 'Hash of original value (deterministic key)',
  masked_value STRING COMMENT 'Masked/tokenized value (format-preserving)',
  encrypted_value STRING COMMENT 'AES-encrypted original value (reversible)',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_identifier PRIMARY KEY (identifier_type, original_hash)
)
CLUSTER BY (identifier_type, original_hash)
COMMENT 'NAB TDM: Cross-system identifier consistency mapping'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'tdm.scope' = 'all_identifiers',
  'tdm.version' = '2024-10-17'
);

-- ==============================================================================
-- VIEWS FOR LOOKUP
-- ==============================================================================

-- Helper view for name lookups
CREATE OR REPLACE VIEW v_masked_names AS
SELECT 
  party_key,
  first_name,
  middle_name,
  last_name,
  full_name,
  'INDIVIDUAL' AS party_type,
  name_hash
FROM name_mapping_individual
UNION ALL
SELECT 
  party_key,
  organization_name AS first_name,
  trading_name AS middle_name,
  NULL AS last_name,
  organization_name AS full_name,
  'ORGANIZATION' AS party_type,
  name_hash
FROM name_mapping_organization;

-- Helper view for address lookups
CREATE OR REPLACE VIEW v_masked_addresses AS
SELECT 
  party_key,
  CONCAT_WS(' ', street_number, street_name) AS masked_street_address,
  property_name,
  unit_number,
  address_hash
FROM address_mapping;

-- ==============================================================================
-- MAINTENANCE UTILITIES
-- ==============================================================================

-- View to check mapping coverage
CREATE OR REPLACE VIEW v_mapping_stats AS
SELECT 
  'individual_names' AS mapping_type,
  COUNT(*) AS total_mappings,
  COUNT(DISTINCT name_hash) AS unique_original_values,
  MAX(created_at) AS last_updated
FROM name_mapping_individual
UNION ALL
SELECT 
  'organization_names',
  COUNT(*),
  COUNT(DISTINCT name_hash),
  MAX(created_at)
FROM name_mapping_organization
UNION ALL
SELECT 
  'addresses',
  COUNT(*),
  COUNT(DISTINCT address_hash),
  MAX(created_at)
FROM address_mapping
UNION ALL
SELECT 
  'identifiers',
  COUNT(*),
  COUNT(DISTINCT original_hash),
  MAX(created_at)
FROM identifier_mapping;

-- Query to check mapping statistics
SELECT * FROM v_mapping_stats ORDER BY mapping_type;
