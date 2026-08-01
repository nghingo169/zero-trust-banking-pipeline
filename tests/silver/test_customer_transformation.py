# Databricks notebook source
"""Unit tests for pipeline.silver.customer_transformation module.

Tests helper functions (get_source_system, resolve_party_status, tokenize_pii, hash_key)
and builder functions (_build_party, _build_party_identifier, _build_party_profile_version, etc.)
using mock Spark DataFrames.
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

# ------------------------------------------------------------------------------
# 3. IMPORT MODULE TỪ PIPELINE/SILVER
# ------------------------------------------------------------------------------
from pipeline.silver.customer_transformation import (
    hash_key,
    tokenize_pii,
    bronze_ref,
    get_source_system,
    resolve_party_status,
    get_pipeline_run_id,
    _build_party_identifier,
    _build_party_identity_resolution,
    _build_party_profile_version,
    _build_party_kyc_assessment,
    _build_party_employment,
    _build_party_service_request,
    AES_KEY
)


# ==============================================================================
# SECTION 1: HELPER FUNCTIONS TESTS
# ==============================================================================

def test_get_source_system_resolves_prefixes_correctly(test_spark):
    """Verify get_source_system maps prefixes (CB, CRM) to labels or defaults to UNKNOWN."""
    df = test_spark.createDataFrame([
        ("CB-1001",),
        ("CRM-2002",),
        ("UNKNOWN-3003",),
        ("INVALID",)
    ], ["ref_id"])

    result_df = df.select(
        get_source_system("ref_id").alias("resolved_system")
    )
    rows = result_df.collect()

    assert rows[0].resolved_system == "CORE_BANKING"
    assert rows[1].resolved_system == "CRM"
    assert rows[2].resolved_system == "UNKNOWN"
    assert rows[3].resolved_system == "UNKNOWN"


def test_resolve_party_status_logic(test_spark):
    """Verify resolve_party_status sets status based on months since last transaction."""
    as_of_time = datetime(2026, 1, 1, 0, 0, 0)
    
    df = test_spark.createDataFrame([
        (datetime(2025, 8, 1, 0, 0, 0),),   # ~5 months ago -> ACTIVE
        (datetime(2023, 1, 1, 0, 0, 0),),   # ~36 months ago -> PENDING
        (datetime(2020, 1, 1, 0, 0, 0),),   # >36 months ago -> DEACTIVE
        (None,)                             # No txn -> PENDING
    ], ["last_txn_at"])

    result_df = df.select(
        resolve_party_status(F.col("last_txn_at"), F.lit(as_of_time)).alias("status")
    )
    statuses = [r.status for r in result_df.collect()]

    assert statuses == ["ACTIVE", "PENDING", "DEACTIVE", "PENDING"]


def test_tokenize_pii_generates_salted_sha256(test_spark):
    """Verify tokenize_pii generates 64-char hex string for customer PII."""
    df = test_spark.createDataFrame([("0901234567",)], ["phone"])
    
    result_df = df.select(tokenize_pii("phone").alias("phone_token"))
    token = result_df.first().phone_token

    assert len(token) == 64
    assert token != "0901234567"


# ==============================================================================
# SECTION 2: PARTY IDENTIFIER & PROFILE VERSION TESTS (PII & AES)
# ==============================================================================

def test_build_party_identifier_unions_national_id_and_phone(test_spark):
    """Verify _build_party_identifier explodes and encrypts NATIONAL_ID and PHONE rows."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("national_id", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("CB-1001", "123456789", "0901234567", "2024-01-01T00:00:00", "RUN_001")
    ], schema)

    with patch("pipeline.silver.customer_transformation.mask_national_id") as mock_mask_nat, \
         patch("pipeline.silver.customer_transformation.mask_phone") as mock_mask_phone:

        mock_mask_nat.return_value = F.lit("123XXXX89")
        mock_mask_phone.return_value = F.lit("090XXXX567")

        result_df = _build_party_identifier(raw_df)
        rows = result_df.collect()

        assert len(rows) == 2, "Should create 2 identifier records (1 for NATIONAL_ID, 1 for PHONE)"

        id_types = {r.identifier_type for r in rows}
        assert id_types == {"NATIONAL_ID", "PHONE"}

        nat_row = next(r for r in rows if r.identifier_type == "NATIONAL_ID")
        assert nat_row.is_primary is True
        assert nat_row.source_system == "CORE_BANKING"
        assert nat_row.identifier_value_masked == "123XXXX89"
        assert len(nat_row.identifier_value_token) == 64


def test_build_party_profile_version_scd2_handling(test_spark):
    """Verify _build_party_profile_version processes SCD2 versions and marks current record."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("address", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("__END_AT", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_data = [
        ("CB-1001", "John Doe", "1990-01-01", "123 Street", "2024-01-01 00:00:00", "2024-06-01 00:00:00", "RUN_01"),
        ("CB-1001", "John Doe Jr", "1990-01-01", "456 Avenue", "2024-06-01 00:00:00", None, "RUN_02")
    ]
    raw_df = test_spark.createDataFrame(raw_data, schema)

    with patch("pipeline.silver.customer_transformation.mask_name") as mock_name, \
         patch("pipeline.silver.customer_transformation.mask_address") as mock_addr:

        mock_name.return_value = F.lit("J*** D**")
        mock_addr.return_value = F.lit("456 A******")

        result_df = _build_party_profile_version(raw_df)
        rows = result_df.collect()

        assert len(rows) == 2

        historical_row = next(r for r in rows if r.effective_to is not None)
        active_row = next(r for r in rows if r.effective_to is None)

        assert historical_row.is_current is False
        assert active_row.is_current is True
        assert active_row.source_system == "CORE_BANKING"


# ==============================================================================
# SECTION 3: OTHER BUILDER TESTS (KYC, EMPLOYMENT, IDENTITY RESOLUTION)
# ==============================================================================

def test_build_party_kyc_assessment(test_spark):
    """Verify _build_party_kyc_assessment maps KYC verification metrics."""
    schema = StructType([
        StructField("kyc_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("id_type", StringType(), True),
        StructField("id_number", StringType(), True),
        StructField("verification_status", StringType(), True),
        StructField("verified_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("KYC_01", "CB-1001", "PASSPORT", "A1234567", "VERIFIED", "2024-02-01", "RUN_01")
    ], schema)

    result_df = _build_party_kyc_assessment(raw_df)
    row = result_df.first()

    assert row.source_business_key == "KYC_01"
    assert row.verification_status == "VERIFIED"
    assert row.source_system == "CORE_BANKING"
    assert len(row.id_number_token) == 64


def test_build_party_employment_casts_income(test_spark):
    """Verify _build_party_employment converts monthly_income to Decimal(12,2)."""
    schema = StructType([
        StructField("employment_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("employer_name", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("monthly_income", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("EMP_01", "CB-1001", "NAB Bank", "Data Engineer", "7500.50", "2024-01-01", "RUN_01")
    ], schema)

    result_df = _build_party_employment(raw_df)
    row = result_df.first()

    assert row.monthly_income == pytest.approx(7500.50)
    assert row.effective_from == date(2024, 1, 1)


def test_build_party_identity_resolution(test_spark):
    """Verify _build_party_identity_resolution maps confidence scores correctly."""
    schema = StructType([
        StructField("party_id", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("CRM-9001", "2024-03-01T10:00:00", "RUN_01")
    ], schema)

    result_df = _build_party_identity_resolution(raw_df)
    row = result_df.first()

    assert row.resolution_status == "CONFIRMED"
    assert row.match_confidence == pytest.approx(1.0000)
    assert row.source_system == "CRM"


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])