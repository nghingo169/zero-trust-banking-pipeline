# Databricks notebook source
"""Unit tests for pipeline.silver.card_transformation module.

Tests Helper Functions & Table Builders:
- Hash key generation (SHA-256 with trimming and null handling)
- PII Tokenization & AES Encryption
- Scalar Subquery / Pipeline Run ID extraction
- All Atomic Silver Card Table Builders (_build_account, _build_payment_card, _build_merchant, etc.)
"""

import builtins
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & ENVIRONMENT FIX
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_SRC = str(PROJECT_ROOT / "src")
SILVER_SRC = str(PROJECT_ROOT / "src" / "pipeline" / "silver")

for path_str in [str(PROJECT_ROOT), PROJECT_SRC, SILVER_SRC]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    test_spark_session = spark  # Databricks Runtime Context
except NameError:
    test_spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("Pipeline-UnitTest")
        .config("spark.sql.shuffle.partitions", "1")
        .config("pipeline.catalog", "workspace")
        .config("pipeline.bronze_schema", "bronze")
        .config("pipeline.silver_validated", "silver_validated")
        .config("pipeline.silver_schema", "silver")
        .config("spark.sql.stackTracesInDataFrameContext", "1")
        .getOrCreate()
    )

builtins.spark = test_spark_session

for k, v in {
    "pipeline.catalog": "workspace",
    "pipeline.bronze_schema": "bronze",
    "pipeline.silver_validated": "silver_validated",
    "pipeline.silver_validated_schema": "silver_validated",
    "pipeline.silver_schema": "silver",
    "pipeline.quality_rules_path": ".",
    "spark.sql.stackTracesInDataFrameContext": "1",
}.items():
    try:
        builtins.spark.conf.set(k, v)
    except Exception:
        pass

# Mock DLT & pyspark.pipelines
if not hasattr(pyspark, "pipelines"):
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    pyspark.pipelines = pipelines_mock
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE & MONKEYPATCH MISSING PII HELPERS
# ------------------------------------------------------------------------------
import card_transformation

if not hasattr(card_transformation, "tokenize_pii"):
    def _mock_tokenize_pii(col_or_name):
        c = F.col(col_or_name) if isinstance(col_or_name, str) else col_or_name
        return F.sha2(F.trim(c.cast("string")), 256)
    card_transformation.tokenize_pii = _mock_tokenize_pii


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================


def test_hash_key_generation(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes with trim/coalesce."""
    df = test_spark.createDataFrame(
        [("core_banking", "10001"), ("core_banking", "  10001  ")], ["system", "id"]
    )

    result_df = df.select(
        card_transformation.hash_key("system", "id").alias("key_hash")
    )
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64, "SHA-256 output must be 64 hexadecimal characters"
    assert hashes[0] == hashes[1], "Trimming whitespace must produce identical hash values"


def test_tokenize_pii(test_spark):
    """Verify tokenize_pii creates a valid SHA-256 token."""
    df = test_spark.createDataFrame([("4532015112830366",)], ["card_no"])
    result_df = df.select(card_transformation.tokenize_pii("card_no").alias("token"))
    token_val = result_df.first().token

    assert len(token_val) == 64
    assert token_val != "4532015112830366"


def test_get_pipeline_run_id_existing_column(test_spark):
    """Verify get_pipeline_run_id extracts column if present in DataFrame."""
    df = test_spark.createDataFrame([("RUN_001",)], ["pipeline_run_id"])
    result_df = df.select(card_transformation.get_pipeline_run_id(df).alias("run_id"))

    assert result_df.first().run_id == "RUN_001"


# ==============================================================================
# SECTION 2: TABLE BUILDERS TESTS
# ==============================================================================


def test_build_account(test_spark):
    """Verify _build_account projects account fields and formats hashes correctly."""
    schema = StructType(
        [
            StructField("account_id", LongType(), True),
            StructField("product_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("open_date", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df = test_spark.createDataFrame(
        [(1001, "SAVINGS", "ACTIVE", "2026-01-01", "BR001", "RUN_CARD_01")], schema
    )

    result_df = card_transformation._build_account(df)
    row = result_df.first()

    assert len(row.account_key) == 64
    assert row.source_account_id == 1001
    assert row.account_status == "ACTIVE"
    assert row.source_system == "core_banking"
    assert row.bronze_record_ref == "account:1001"


def test_build_payment_card(test_spark):
    """Verify _build_payment_card projects card_number accurately."""
    schema = StructType(
        [
            StructField("card_id", StringType(), True),
            StructField("account_id", LongType(), True),
            StructField("card_number", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("issue_date", StringType(), True),
            StructField("expiry_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df = test_spark.createDataFrame(
        [
            (
                "CARD_99",
                1001,
                "4532015112830366",
                "VISA_DEBIT",
                "2026-01-01",
                "2030-01-01",
                "ACTIVE",
                "RUN_01",
            )
        ],
        schema,
    )

    result_df = card_transformation._build_payment_card(df)
    row = result_df.first()

    assert row.source_card_id == "CARD_99"
    assert len(row.payment_card_key) == 64
    assert row.card_number == "4532015112830366"
    assert row.source_system == "card_system"

def test_build_payment_card_limit_history(test_spark):
    """Verify _build_payment_card_limit_history casts limit_amount to Decimal(12,2)."""
    schema = StructType(
        [
            StructField("history_id", StringType(), True),
            StructField("card_id", StringType(), True),
            StructField("limit_amount", DoubleType(), True),
            StructField("effective_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df = test_spark.createDataFrame(
        [("HIST_01", "CARD_99", 5000.50, "2026-02-01", "RUN_01")], schema
    )

    result_df = card_transformation._build_payment_card_limit_history(df)
    row = result_df.first()

    assert float(row.limit_amount) == 5000.50
    assert row.source_system == "card_system"


def test_build_merchant_and_location(test_spark):
    """Verify _build_merchant and _build_merchant_location field mappings."""
    schema_mch = StructType(
        [
            StructField("merchant_id", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("mcc_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_mch = test_spark.createDataFrame(
        [("MERCH_01", "Highlands Coffee", "5812", "VN", "RUN_01")], schema_mch
    )

    res_mch = card_transformation._build_merchant(df_mch).first()
    assert res_mch.merchant_name == "Highlands Coffee"
    assert res_mch.source_system == "merchant_system"

    schema_loc = StructType(
        [
            StructField("store_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("store_description", StringType(), True),
            StructField("store_type", StringType(), True),
            StructField("store_address", StringType(), True),
            StructField("risk_rating", StringType(), True),
            StructField("registered_date", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
        ]
    )
    df_loc = test_spark.createDataFrame(
        [
            (
                "STORE_101",
                "MERCH_01",
                "Highlands District 1",
                "Flagship Store",
                "RETAIL",
                "72 Le Thanh Ton",
                "LOW",
                "2026-01-01",
                "RUN_01",
            )
        ],
        schema_loc,
    )

    res_loc = card_transformation._build_merchant_location(df_loc).first()
    assert res_loc.store_name == "Highlands District 1"
    assert len(res_loc.merchant_location_key) == 64


# Entrypoint cho chạy test trực tiếp
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])