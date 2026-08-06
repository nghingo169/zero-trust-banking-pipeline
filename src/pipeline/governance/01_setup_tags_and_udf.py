# Databricks notebook source
# =============================================================================
# ZERO-TRUST BANKING PIPELINE - FULL 39 TDM RULES GOVERNANCE SETUP (PYTHON NOTEBOOK)
# Description: Creates governance schema, defines PII tags, and comprehensive TDM UDF Engine.
# Author: Data Engineering Squad
# =============================================================================
# -----------------------------------------------------------------------------
# BƯỚC 0: TẠO GOVERNED TAG KEY (BẮT BUỘC TRƯỚC KHI GÁN TAGS)
# -----------------------------------------------------------------------------
print("Creating governed tag: pii_type...")
try:
    spark.sql("""
    CREATE GOVERNED TAG pii_type
    DESCRIPTION 'Classifies columns containing personally identifiable information for TDM masking'
    VALUES (
        'individual_name',
        'address', 
        'national_id',
        'org_name',
        'narrative',
        'card_number',
        'store_name',
        'phone'
    )
    """)
    print("Governed tag 'pii_type' created successfully.")
except Exception as e:
    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
        print("Governed tag 'pii_type' already exists, continuing...")
    else:
        # Re-raise if it's a different error (e.g., permission denied)
        raise
# Lấy Catalog từ Spark Config (mặc định 'workspace' nếu chạy đơn lẻ)
try:
    catalog = spark.conf.get("pipeline.catalog", "workspace")
except Exception:
    catalog = "workspace"

print(f"Executing Governance Setup on Catalog: {catalog}")

# 1. Khởi tạo Schema Quản trị dữ liệu
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.governance
COMMENT 'Dedicated schema for Data Governance, Audit Logs, and Security Masking UDFs'
""")

# 3. Đóng gói trọn bộ 39 NAB TDM Masking Rules thành SQL UDF
udf_script = f"""
CREATE OR REPLACE FUNCTION {catalog}.governance.tdm_masking_engine(
  val STRING, 
  pii_type STRING
)
RETURNS STRING
COMMENT 'Comprehensive 39-Rule NAB TDM Engine for Unity Catalog ABAC Dynamic Column Masking'
RETURN CASE
  -- Nguyên tắc chung TDM: Giữ nguyên nếu dữ liệu rỗng (NULL hoặc Blank)
  WHEN val IS NULL OR TRIM(val) = '' THEN val

  -- ---------------------------------------------------------------------------
  -- GROUP 1: PERSONAL IDENTIFIERS & NAMES
  -- ---------------------------------------------------------------------------
  -- Rule 1.1: NIN (National Identification Number)
  WHEN pii_type = 'nin' THEN 
    CONCAT('011', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 7))

  -- Rule 1.2: Individual Name -> J. MASKED_8F3A12
  WHEN pii_type = 'individual_name' THEN 
    CONCAT(UPPER(LEFT(TRIM(val), 1)), '. MASKED_', UPPER(SUBSTRING(MD5(val), 1, 6)))

  -- Rule 1.3: Organization Names / Store Names (Rule 1.30: EB Name)
  WHEN pii_type = 'org_name' OR pii_type = 'store_name' THEN 
    CONCAT('ORG_', UPPER(SUBSTRING(MD5(val), 1, 8)))

  -- Rule 1.4: Date of Birth or Date of Death -> Keep Year
  WHEN pii_type = 'dob' OR pii_type = 'dod' THEN 
    CONCAT(LEFT(val, 4), '-01-01')

  -- ---------------------------------------------------------------------------
  -- GROUP 2: GOVERNMENT & TAX IDENTIFIERS
  -- ---------------------------------------------------------------------------
  -- Rule 1.5: ABN Modulus 89 representation
  WHEN pii_type = 'abn' THEN '10123456789'

  -- Rule 1.6: ACN / ARBN / ARSN Modulus 10 representation
  WHEN pii_type = 'acn' THEN '012345674'

  -- Rule 1.7: TFN Modulus 11 representation
  WHEN pii_type = 'tfn' THEN '123456782'

  -- Rule 1.8: Driving License Number
  WHEN pii_type = 'driver_license' THEN 
    CONCAT('DL', UPPER(SUBSTRING(MD5(val), 1, 7)))

  -- Rule 1.9: Medicare Card Number
  WHEN pii_type = 'medicare' THEN 
    CONCAT('2', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 9))

  -- Rule 1.10: Passport Number
  WHEN pii_type = 'passport' THEN 
    CONCAT('N', UPPER(SUBSTRING(MD5(val), 1, 8)))

  -- Rule 1.18, 1.19: TIN / EIN
  WHEN pii_type = 'tin' OR pii_type = 'ein' THEN 
    CONCAT(LEFT(val, 1), RIGHT(REGEXP_REPLACE(MD5(val), '[^0-9]', ''), 8))

  -- Rule 1.34: Birth Certificate Number
  WHEN pii_type = 'birth_certificate' THEN 
    CONCAT('BC', UPPER(SUBSTRING(MD5(val), 1, 8)))

  -- ---------------------------------------------------------------------------
  -- GROUP 3: CONTACT & LOCATION DETAILS
  -- ---------------------------------------------------------------------------
  -- Rule 1.11: Address
  WHEN pii_type = 'address' THEN '456 Masked Street, MASKED_SUBURB NSW 2789'

  -- Rule 1.12: Phone Number
  WHEN pii_type = 'phone' THEN 
    CONCAT(LEFT(REGEXP_REPLACE(val, '[^0-9]', ''), 4), '11', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4))

  -- Rule 1.13: Email Address
  WHEN pii_type = 'email' THEN 
    CONCAT(LOWER(SUBSTRING(MD5(val), 1, 10)), '@notreal.nab.com.au')

  -- Rule 1.37: GPS Location Coordinates
  WHEN pii_type = 'gps_coordinates' THEN '-37.8136, 144.9631'

  -- ---------------------------------------------------------------------------
  -- GROUP 4: BANKING, CARDS & FINANCIAL IDENTIFIERS
  -- ---------------------------------------------------------------------------
  -- Rule 1.14: Arrangement / Account Short Name
  WHEN pii_type = 'account_short_name' THEN 'MASKED ACC SHORTNAME'

  -- Rule 1.15: Card Numbers -> BIN + Flipped Checksum
  WHEN pii_type = 'card_number' THEN 
    CONCAT(LEFT(val, 9), '999999', RIGHT(val, 1))

  -- Rule 1.28: Cheque Number
  WHEN pii_type = 'cheque_number' THEN 
    CONCAT(LEFT(val, 1), '999999', SUBSTRING(val, 8, 10))

  -- Rule 1.29: Swift Code / BIC Code
  WHEN pii_type = 'swift_code' THEN 
    CONCAT(LEFT(val, 7), '0', SUBSTRING(val, 9, 10))

  -- Rule 1.33: IBAN
  WHEN pii_type = 'iban' THEN 
    CONCAT(LEFT(val, 4), ' 0000 0000 0000 0000')

  -- Rule 1.35: Superannuation Account
  WHEN pii_type = 'superannuation' THEN 'APIR9999AU'

  -- ---------------------------------------------------------------------------
  -- GROUP 5: FINANCIAL MARKET & REGULATORY IDENTIFIERS
  -- ---------------------------------------------------------------------------
  -- Rule 1.20, 1.21, 1.22: GIIN / HIN / SRN
  WHEN pii_type = 'giin' OR pii_type = 'hin' OR pii_type = 'srn' THEN 
    CONCAT('X', UPPER(SUBSTRING(MD5(val), 1, 9)))

  -- Rule 1.31: LEI / CICI
  WHEN pii_type = 'lei' THEN 
    CONCAT(LEFT(val, 6), UPPER(SUBSTRING(MD5(val), 1, 12)), '97')

  -- Rule 1.32: AVID
  WHEN pii_type = 'avid' THEN 
    UPPER(SUBSTRING(MD5(val), 1, 8))

  -- Rule 1.39: Key Identifiers
  WHEN pii_type = 'key_identifier' THEN 
    CONCAT('ID_', UPPER(SUBSTRING(MD5(val), 1, 8)))

  -- ---------------------------------------------------------------------------
  -- GROUP 6: DIGITAL, SECURITY & UNSTRUCTURED DATA
  -- ---------------------------------------------------------------------------
  -- Rule 1.16: Narratives / Description / Comments
  WHEN pii_type = 'narrative' THEN 'PRIVATIZED TEXT'

  -- Rule 1.17: Images
  WHEN pii_type = 'image' THEN 'IMAGE_PRIVATIZED_BLOB'

  -- Rule 1.23: Web Address (URL)
  WHEN pii_type = 'web_address' THEN 'HTTP://www.99999.notreal.com.au'

  -- Rule 1.24: IP Address
  WHEN pii_type = 'ip_address' THEN '127.0.0.1'

  -- Rule 1.25: Passwords
  WHEN pii_type = 'password' THEN '********'

  -- Rule 1.26: Vehicle Identification Number (VIN)
  WHEN pii_type = 'vin' THEN 
    CONCAT(LEFT(val, 3), 'XXXXX', SUBSTRING(val, 9, 3), 'XXXXXX')

  -- Rule 1.27: Security Questions & Answers
  WHEN pii_type = 'security_qa' THEN 'PRIVATIZED SECURITY ANSWER'

  -- Rule 1.36: Cookies / Session ID
  WHEN pii_type = 'cookie_session' THEN 'SESSION_BLANKED_OUT'

  -- Rule 1.38: File Paths
  WHEN pii_type = 'file_path' THEN '/test_env/masked_filepath.dat'

  -- Default Fallback
  ELSE '***PRIVATIZED***'
END;
"""

spark.sql(udf_script)
print("Successfully initialized 39 TDM Rules Engine.")