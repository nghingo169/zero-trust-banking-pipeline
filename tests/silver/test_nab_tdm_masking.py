# Databricks notebook source
"""Unit tests for nab_tdm_masking module.

Tests NAB TDM Masking Logic & Referential Integrity:
- Card Number Masking (NAB Rule 1.15)
- National ID Masking (NAB Rule 1.12)
- Phone Number Masking (NAB Rule 1.13)
- Name Masking (NAB Rule 1.10)
- Address Masking (NAB Rule 1.11)
- Referential Integrity Verification (Determinism check across multiple calls)
"""

from pathlib import Path
import os
import sys
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
TESTS_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, TESTS_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE (SPARK CONF ONLY)
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    """Provides active Databricks SparkSession or builds local fallback using spark.conf."""
    try:
        session = spark  # type: ignore # noqa: F821
    except NameError:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("NAB-TDM-Masking-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        import builtins
        builtins.spark = session

    return session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
try:
    import nab_tdm_masking
except ImportError:
    from src import nab_tdm_masking  # Fallback cho đường dẫn đĩa src/


# ==============================================================================
# SECTION 1: CORE FUNCTIONALITY & REFERENTIAL INTEGRITY TESTS
# ==============================================================================

def test_masking_referential_integrity_determinism(test_spark):
    """Verify that same inputs produce identical masked outputs across multiple calls."""
    schema = StructType([StructField("raw_value", StringType(), True)])
    df = test_spark.createDataFrame([
        ("John Smith",),
        ("John Smith",)  # Duplicate row to verify determinism
    ], schema)

    res_df = df.select(
        nab_tdm_masking.mask_name("raw_value").alias("masked_name"),
        nab_tdm_masking.mask_phone(F.lit("0412345678")).alias("masked_phone")
    )
    rows = res_df.collect()

    # Rule 1: Output phải giống hệt nhau với cùng một đầu vào
    assert rows[0].masked_name == rows[1].masked_name
    assert rows[0].masked_phone == rows[1].masked_phone


# ==============================================================================
# SECTION 2: NAB MASKING RULES UNIT TESTS
# ==============================================================================

def test_mask_card_number_rule_1_15(test_spark):
    """Verify Card Number Masking (NAB Rule 1.15) retains BIN & length while masking middle digits."""
    schema = StructType([StructField("card_no", StringType(), True)])
    data = [
        ("4532015112830366",),  # 16-digit card
        ("378282246310005",)   # 15-digit card (Amex)
    ]
    df = test_spark.createDataFrame(data, schema)

    res_df = df.select(
        F.col("card_no"),
        nab_tdm_masking.mask_card_number("card_no").alias("masked_card")
    )
    rows = res_df.collect()

    # 16-digit card test
    c16_orig, c16_masked = rows[0].card_no, rows[0].masked_card
    assert len(c16_masked) == 16, "Masked card length must match original length"
    assert c16_masked[:9] == c16_orig[:9], "First 9 digits (BIN 6 + NAB 3) must be retained"
    assert c16_masked != c16_orig, "Masked card must not equal original card"

    # 15-digit card test
    c15_orig, c15_masked = rows[1].card_no, rows[1].masked_card
    assert len(c15_masked) == 15
    assert c15_masked[:9] == c15_orig[:9]


def test_mask_national_id_rule_1_12(test_spark):
    """Verify National ID Masking (NAB Rule 1.12) retains prefix/suffix 3 digits."""
    schema = StructType([StructField("nat_id", StringType(), True)])
    data = [
        ("123456789",),  # Valid >= 9 digits
        ("123",)        # Short length fallback
    ]
    df = test_spark.createDataFrame(data, schema)

    res_df = df.select(
        F.col("nat_id"),
        nab_tdm_masking.mask_national_id("nat_id").alias("masked_id")
    )
    rows = res_df.collect()

    # >= 9 digits test
    id_orig, id_masked = rows[0].nat_id, rows[0].masked_id
    assert len(id_masked) == 9
    assert id_masked[:3] == id_orig[:3], "Prefix 3 digits must be retained"
    assert id_masked[-3:] == id_orig[-3:], "Suffix 3 digits must be retained"
    assert id_masked != id_orig

    # Short length test
    assert "XXX" in rows[1].masked_id


def test_mask_phone_rule_1_13(test_spark):
    """Verify Phone Masking (NAB Rule 1.13) retains prefix 4 and suffix 4 digits."""
    schema = StructType([StructField("phone", StringType(), True)])
    data = [
        ("0412345678",),  # 10 digits
        ("1234",)        # Short length
    ]
    df = test_spark.createDataFrame(data, schema)

    res_df = df.select(
        F.col("phone"),
        nab_tdm_masking.mask_phone("phone").alias("masked_phone")
    )
    rows = res_df.collect()

    # Valid phone test
    p_orig, p_masked = rows[0].phone, rows[0].masked_phone
    assert len(p_masked) == 10
    assert p_masked[:4] == p_orig[:4], "First 4 digits (area code) must be retained"
    assert p_masked[-4:] == p_orig[-4:], "Last 4 digits must be retained"
    assert p_masked != p_orig

    # Short phone test
    assert "XXXX" in rows[1].masked_phone


def test_mask_name_rule_1_10(test_spark):
    """Verify Name Masking (NAB Rule 1.10) retains initial and adds MASKED hash suffix."""
    schema = StructType([StructField("full_name", StringType(), True)])
    df = test_spark.createDataFrame([("John Smith",), ("alice",)], schema)

    res_df = df.select(
        F.col("full_name"),
        nab_tdm_masking.mask_name("full_name").alias("masked_name")
    )
    rows = res_df.collect()

    # Row 1: "John Smith" -> "J. MASKED_XXXXXX"
    m1 = rows[0].masked_name
    assert m1.startswith("J. MASKED_")
    assert len(m1) == 16  # 1 (Initial) + 2 (". ") + 7 ("MASKED_") + 6 (Hash)

    # Row 2: "alice" -> "A. MASKED_XXXXXX"
    m2 = rows[1].masked_name
    assert m2.startswith("A. MASKED_")


def test_mask_address_rule_1_11(test_spark):
    """Verify Address Masking (NAB Rule 1.11) generates standard format masked address."""
    schema = StructType([StructField("address", StringType(), True)])
    df = test_spark.createDataFrame([("123 Real St, Sydney NSW 2000",)], schema)

    res_df = df.select(
        nab_tdm_masking.mask_address("address").alias("masked_address")
    )
    row = res_df.first()
    m_addr = row.masked_address

    assert "Masked Street, MASKED_SUBURB NSW 2" in m_addr
    assert len(m_addr) >= 42  # Kiểm tra độ dài định dạng tiêu chuẩn


# Entrypoint thực thi trực tiếp từ file
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])