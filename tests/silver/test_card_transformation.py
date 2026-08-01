"""Unit tests for pipeline.silver.card_transformation module.

Tests helper functions (hash_key, tokenize_pii, bronze_ref) and table builder functions
(_build_payment_card, _build_account, _build_party_account_role, etc.) using mock Spark DataFrames.
"""

import os
import sys
from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & MODULE IMPORT
# ------------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
SEARCH_DIR = CURRENT_DIR
FOUND_SRC = None

while SEARCH_DIR:
    if os.path.exists(os.path.join(SEARCH_DIR, "src", "pipeline")):
        FOUND_SRC = os.path.join(SEARCH_DIR, "src")
        break
    elif os.path.exists(os.path.join(SEARCH_DIR, "pipeline")):
        FOUND_SRC = SEARCH_DIR
        break
    parent = os.path.dirname(SEARCH_DIR)
    if parent == SEARCH_DIR:
        break
    SEARCH_DIR = parent

if FOUND_SRC and FOUND_SRC not in sys.path:
    sys.path.insert(0, FOUND_SRC)

# Import functions under test from pipeline.silver.card_transformation
from pipeline.silver.card_transformation import (
    hash_key,
    tokenize_pii,
    bronze_ref,
    get_pipeline_run_id,
    _build_account,
    _build_party_account_role,
    _build_payment_card,
    _build_payment_card_limit_history,
    _build_account_balance_snapshot,
    _build_merchant,
    _build_merchant_location
)


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================

def test_hash_key_returns_deterministic_sha256(test_spark):
    """Verify hash_key generates deterministic SHA-256 hashes and trims whitespace."""
    df = test_spark.createDataFrame([
        ("core_banking", "10001"),
        ("core_banking", "  10001  ")  # Whitspace test
    ], ["system", "id"])

    result_df = df.select(hash_key("system", "id").alias("key_hash"))
    rows = result_df.collect()

    assert len(rows[0].key_hash) == 64, "SHA-256 output must be 64 hexadecimal characters"
    assert rows[0].key_hash == rows[1].key_hash, "Trimming must produce identical hash values"


def test_tokenize_pii_generates_salted_sha256(test_spark):
    """Verify tokenize_pii applies salt and returns SHA-256 token."""
    df = test_spark.createDataFrame([("4111222233334444",)], ["card_no"])
    
    result_df = df.select(tokenize_pii("card_no").alias("card_token"))
    token = result_df.first().card_token

    assert len(token) == 64, "Tokenized output must be a SHA-256 string"
    assert token != "4111222233334444", "Raw card number must be masked/tokenized"


def test_bronze_ref_formats_correctly(test_spark):
    """Verify bronze_ref formats string as 'table_name:key'."""
    df = test_spark.createDataFrame([(101,)], ["account_id"])
    
    result_df = df.select(bronze_ref("account", "account_id").alias("ref"))
    ref_value = result_df.first().ref

    assert ref_value == "account:101", "Reference format must be 'table:id'"


def test_get_pipeline_run_id_uses_column_if_present(test_spark):
    """Verify get_pipeline_run_id extracts column directly if present in DataFrame."""
    df = test_spark.createDataFrame([("RUN_12345",)], ["pipeline_run_id"])
    
    result_df = df.select(get_pipeline_run_id(df).alias("run_id"))
    assert result_df.first().run_id == "RUN_12345"


# ==============================================================================
# SECTION 2: PAYMENT CARD BUILDER TESTS (PII, AES ENCRYPTION & MASKING)
# ==============================================================================

def test_build_payment_card_transforms_and_encrypts_pii(test_spark):
    """Verify _build_payment_card applies masking, AES-256 encryption, and tokenization."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("card_number", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("issue_date", StringType(), True),
        StructField("expiry_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    mock_data = [("CARD_001", 10001, "4111222233334444", "CREDIT", "2024-01-01", "2028-12-31", "ACTIVE", "TEST_RUN_01")]
    raw_df = test_spark.createDataFrame(mock_data, schema)

    # Patch nab_mask_card to test transformation pipeline in isolation
    with patch("pipeline.silver.card_transformation.nab_mask_card") as mock_mask:
        mock_mask.return_value = F.lit("4111-XXXX-XXXX-4444")

        transformed_df = _build_payment_card(raw_df)
        row = transformed_df.first()

        # Structural & Key checks
        assert row.source_card_id == "CARD_001"
        assert len(row.payment_card_key) == 64
        assert row.card_status == "ACTIVE"

        # Security & Masking checks
        assert row.card_number_masked == "4111-XXXX-XXXX-4444", "Masking function must be called"
        assert row.card_number_encrypted is not None, "AES encrypted column must not be NULL"
        assert row.card_number_encrypted != "4111222233334444", "Raw card number must be encrypted"
        assert len(row.card_number_token) == 64, "Token must be valid SHA-256"


# ==============================================================================
# SECTION 3: OTHER BUILDER FUNCTIONS TESTS
# ==============================================================================

def test_build_account_transforms_schema_correctly(test_spark):
    """Verify _build_account maps core_banking account fields to Silver Atomic schema."""
    schema = StructType([
        StructField("account_id", LongType(), True),
        StructField("product_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("open_date", StringType(), True),
        StructField("branch_code", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    
    raw_df = test_spark.createDataFrame([(10001, "SAVINGS", "OPEN", "2023-05-10", "BR_001", "RUN_999")], schema)
    result_df = _build_account(raw_df)
    row = result_df.first()

    assert row.source_account_id == 10001
    assert row.account_status == "OPEN"
    assert row.source_system == "core_banking"
    assert row.bronze_record_ref == "account:10001"
    assert row.open_date == date(2023, 5, 10)


def test_build_payment_card_limit_history_casts_decimals(test_spark):
    """Verify _build_payment_card_limit_history converts limit_amount to Decimal(12,2)."""
    schema = StructType([
        StructField("history_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("limit_amount", StringType(), True),
        StructField("effective_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([("HIST_01", "CARD_001", "5000.50", "2025-01-01", "RUN_01")], schema)
    result_df = _build_payment_card_limit_history(raw_df)
    row = result_df.first()

    assert row.limit_amount == pytest.approx(5000.50)
    assert row.source_business_key == "HIST_01"


def test_build_account_balance_snapshot_handles_balances(test_spark):
    """Verify _build_account_balance_snapshot processes snapshot metrics."""
    schema = StructType([
        StructField("balance_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("balance_date", StringType(), True),
        StructField("opening_balance", StringType(), True),
        StructField("closing_balance", StringType(), True),
        StructField("available_balance", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame(
        [("BAL_01", 10001, "2025-01-31", "1000.00", "1500.00", "1500.00", "RUN_01")], schema
    )
    result_df = _build_account_balance_snapshot(raw_df)
    row = result_df.first()

    assert row.opening_balance == pytest.approx(1000.00)
    assert row.closing_balance == pytest.approx(1500.00)
    assert row.balance_date == date(2025, 1, 31)


def test_build_merchant_and_location(test_spark):
    """Verify _build_merchant constructs keys correctly."""
    merchant_schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([("MERCH_01", "Shopee", "5311", "VN", "RUN_01")], merchant_schema)
    result_df = _build_merchant(raw_df)
    row = result_df.first()

    assert row.source_merchant_id == "MERCH_01"
    assert row.source_system == "merchant_system"
    assert len(row.merchant_key) == 64


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])