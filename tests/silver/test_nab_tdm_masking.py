# Databricks notebook source
"""Unit tests for pipeline.silver.nab_tdm_masking module.

Tests NAB TDM Format-Preserving Encryption & Masking Functions:
- mask_card_number
- mask_national_id
- mask_phone
- mask_name
- mask_address
- Check digit & Hash helper functions (_deterministic_hash, _deterministic_random, _deterministic_choice, _calculate_luhn_checksum)
"""

from pathlib import Path
import os
import sys
import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & DLT MOCK
# ------------------------------------------------------------------------------
# Thêm đường dẫn 'src' vào sys.path để Python nhận diện được package 'pipeline.silver'
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT module nếu chạy dưới dạng Unit Test độc lập
try:
    import dlt
except ImportError:
    from types import ModuleType
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock pyspark.pipelines để tránh lỗi PIPELINES_NOT_SUPPORTED trên local
if "pyspark.pipelines" not in sys.modules:
    from types import ModuleType
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

# ------------------------------------------------------------------------------
# 2. LOCAL SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    """Tự động cấp hoặc khởi tạo SparkSession cho test runner."""
    try:
        # Nếu chạy trên Databricks Notebook / Interactive Cluster
        return spark  # type: ignore # noqa: F821
    except NameError:
        # Nếu chạy Local PyTest trên VS Code
        return (
            SparkSession.builder
            .master("local[1]")
            .appName("NAB-TDM-Masking-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

# ------------------------------------------------------------------------------
# 3. IMPORT MODULE NAB_TDM_MASKING
# ------------------------------------------------------------------------------
try:
    from pipeline.silver.nab_tdm_masking import (
        _deterministic_hash,
        _deterministic_random,
        _deterministic_choice,
        _calculate_luhn_checksum,
        mask_card_number,
        mask_national_id,
        mask_phone,
        mask_name,
        mask_address
    )
except ImportError:
    from nab_tdm_masking import (
        _deterministic_hash,
        _deterministic_random,
        _deterministic_choice,
        _calculate_luhn_checksum,
        mask_card_number,
        mask_national_id,
        mask_phone,
        mask_name,
        mask_address
    )


# ==============================================================================
# SECTION 1: HELPER & CHECK DIGIT ALGORITHM TESTS
# ==============================================================================

def test_deterministic_hash_integrity():
    """Verify same input always yields exact same hash (referential integrity)."""
    hash1 = _deterministic_hash("CUST_1001")
    hash2 = _deterministic_hash("CUST_1001")
    hash3 = _deterministic_hash("CUST_1002")

    assert hash1 == hash2, "Same input must produce identical hash"
    assert hash1 != hash3, "Different inputs should produce different hashes"


def test_deterministic_random_range():
    """Verify deterministic random output stays within expected bounds."""
    val = _deterministic_random("TEST_SEED", min_val=100, max_val=200)
    assert 100 <= val <= 200


def test_deterministic_choice():
    """Verify choice selection is deterministic."""
    choices = ["NSW", "VIC", "QLD", "WA"]
    pick1 = _deterministic_choice("SEED_A", choices)
    pick2 = _deterministic_choice("SEED_A", choices)
    
    assert pick1 == pick2
    assert pick1 in choices


def test_calculate_luhn_checksum():
    """Verify Luhn checksum calculation and flipped checksum logic."""
    # Test card number string
    checksum = _calculate_luhn_checksum("453201511283036")
    assert isinstance(checksum, str)
    assert len(checksum) == 1


# ==============================================================================
# SECTION 2: CARD NUMBER MASKING TESTS (NAB Rule 1.15)
# ==============================================================================

def test_mask_card_number_format_preservation(test_spark):
    """Verify card masking retains first 9 digits and keeps original length."""
    schema = StructType([StructField("card_no", StringType(), True)])
    df = test_spark.createDataFrame([
        ("4532015112830366",),  # 16 digits
        ("378282246310005",)    # 15 digits (Amex)
    ], schema)

    result_df = df.select(
        F.col("card_no").alias("original"),
        mask_card_number("card_no").alias("masked")
    )
    rows = result_df.collect()

    # 16-digit card checks
    card16_orig = rows[0].original
    card16_mask = rows[0].masked
    assert len(card16_mask) == 16, "16-digit card must remain 16 digits"
    assert card16_mask[:9] == card16_orig[:9], "First 9 digits (BIN + NAB integrity) must be retained"
    assert card16_mask != card16_orig, "Masked card must not equal original"

    # 15-digit card checks
    card15_orig = rows[1].original
    card15_mask = rows[1].masked
    assert len(card15_mask) == 15, "15-digit card must remain 15 digits"
    assert card15_mask[:9] == card15_orig[:9]


def test_mask_card_number_referential_integrity(test_spark):
    """Verify same card number masked twice produces identical masked output."""
    df = test_spark.createDataFrame([("4532015112830366",)], ["card_no"])
    
    res1 = df.select(mask_card_number("card_no").alias("masked")).first().masked
    res2 = df.select(mask_card_number("card_no").alias("masked")).first().masked

    assert res1 == res2, "Masking must be deterministic across runs"


# ==============================================================================
# SECTION 3: NATIONAL ID MASKING TESTS (NAB Rule 1.12)
# ==============================================================================

def test_mask_national_id_standard(test_spark):
    """Verify National ID retains prefix 3 and suffix 3 digits."""
    df = test_spark.createDataFrame([("123456789",)], ["nat_id"])
    
    result_df = df.select(mask_national_id("nat_id").alias("masked"))
    masked_val = result_df.first().masked

    assert len(masked_val) == 9
    assert masked_val[:3] == "123", "Must retain first 3 digits"
    assert masked_val[-3:] == "789", "Must retain last 3 digits"
    assert masked_val != "123456789"


def test_mask_national_id_short_input(test_spark):
    """Verify National ID under 9 digits falls back to XXX pattern."""
    df = test_spark.createDataFrame([("12345",)], ["nat_id"])
    
    result_df = df.select(mask_national_id("nat_id").alias("masked"))
    masked_val = result_df.first().masked

    assert masked_val.startswith("XXX")
    assert masked_val.endswith("XXX")


# ==============================================================================
# SECTION 4: PHONE NUMBER MASKING TESTS (NAB Rule 1.13)
# ==============================================================================

def test_mask_phone_standard(test_spark):
    """Verify Phone retains first 4 (area code) and last 4 digits."""
    df = test_spark.createDataFrame([("0412345678",)], ["phone"])
    
    result_df = df.select(mask_phone("phone").alias("masked"))
    masked_val = result_df.first().masked

    assert len(masked_val) == 10
    assert masked_val[:4] == "0412", "Must retain first 4 digits (area code)"
    assert masked_val[-4:] == "5678", "Must retain last 4 digits"
    assert masked_val != "0412345678"


def test_mask_phone_short_input(test_spark):
    """Verify short phone number falls back to XXXX prefix/suffix pattern."""
    df = test_spark.createDataFrame([("123456",)], ["phone"])
    
    result_df = df.select(mask_phone("phone").alias("masked"))
    masked_val = result_df.first().masked

    assert masked_val.startswith("XXXX")
    assert masked_val.endswith("XXXX")


# ==============================================================================
# SECTION 5: NAME & ADDRESS MASKING TESTS (NAB Rules 1.10 & 1.11)
# ==============================================================================

def test_mask_name_pattern(test_spark):
    """Verify Name retains initial letter and appends MASKED hash suffix."""
    df = test_spark.createDataFrame([
        ("John Smith",),
        ("alice williams",)
    ], ["full_name"])

    result_df = df.select(mask_name("full_name").alias("masked"))
    rows = result_df.collect()

    assert rows[0].masked.startswith("J. MASKED_")
    assert rows[1].masked.startswith("A. MASKED_")
    assert len(rows[0].masked.split("_")[1]) == 6, "Hash suffix must be 6 hex characters"


def test_mask_address_pattern(test_spark):
    """Verify Address formats correctly into standard masked street address."""
    df = test_spark.createDataFrame([("123 George St, Sydney NSW 2000",)], ["address"])
    
    result_df = df.select(mask_address("address").alias("masked"))
    masked_val = result_df.first().masked

    assert "Masked Street, MASKED_SUBURB NSW 2" in masked_val
    assert len(masked_val.split(" ")[0]) == 3, "Street number should be 3 digits"


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])