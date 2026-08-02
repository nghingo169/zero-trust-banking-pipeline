"""Unit tests for pipeline.silver.customer_validation module.

Tests Customer Validation Logic:
- Duplicate active national ID resolution (duplicate_national_ids)
- Evaluation of quality rules returned by get_rules(table_name)
- Joining and flagging of duplicate national IDs in assess()
- Handling of null constraint evaluations (coalesced to False)
- Schema normalization invocation via data_contracts.normalization
"""

# Databricks notebook source
from pathlib import Path
import os
import sys
import builtins
from types import ModuleType
from unittest.mock import patch, MagicMock
import pytest

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION
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
        SparkSession.builder
        .master("local[1]")
        .appName("Pipeline-UnitTest")
        .config("spark.sql.shuffle.partitions", "1")
        .config("pipeline.catalog", "workspace")
        .config("pipeline.bronze_schema", "bronze")
        .config("pipeline.silver_validated", "silver_validated")
        .config("pipeline.silver_schema", "silver")
        .getOrCreate()
    )

builtins.spark = test_spark_session

# Set default Spark Configs to prevent AnalysisException on Databricks Connect
for k, v in {
    "pipeline.catalog": "workspace",
    "pipeline.bronze_schema": "bronze",
    "pipeline.silver_validated": "silver_validated",
    "pipeline.silver_validated_schema": "silver_validated",
    "pipeline.silver_schema": "silver",
    "pipeline.quality_rules_path": ".",
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
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import customer_validation


# ==============================================================================
# SECTION 1: DUPLICATE NATIONAL IDS TESTS
# ==============================================================================

def test_duplicate_national_ids_identifies_duplicates(test_spark):
    """Verify duplicate_national_ids returns national_id occurring more than once for active records."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("national_id", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    data = [
        ("CUST_01", "NAT_100", None),
        ("CUST_02", "NAT_100", None),                     # Active duplicate
        ("CUST_03", "NAT_200", None),                     # Unique active
        ("CUST_04", "NAT_300", "2026-01-01T00:00:00"),    # Inactive history
        ("CUST_05", "NAT_300", None)                      # Inactive + Active -> count = 1
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_session = MagicMock()
    mock_session.read.table.return_value = df

    target_module = "pipeline.silver.customer_validation" if "pipeline.silver.customer_validation" in sys.modules else "customer_validation"

    with patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):
        dup_df = customer_validation.duplicate_national_ids(mock_session, "bronze.core_banking_customer")
        
        assert dup_df is not None
        dups = [row.national_id for row in dup_df.collect()]
        assert dups == ["NAT_100"]


def test_duplicate_national_ids_returns_none_for_non_identity_tables(test_spark):
    """Verify duplicate_national_ids returns None for tables not in IDENTITY_TABLES."""
    mock_session = MagicMock()
    res = customer_validation.duplicate_national_ids(mock_session, "bronze.customer_kyc")
    assert res is None


# ==============================================================================
# SECTION 2: CUSTOMER VALIDATION ASSESS TESTS
# ==============================================================================

def test_assess_all_rules_passing(test_spark):
    """Verify that _failed_rule_names is empty when all rules pass and no duplicate IDs exist."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("national_id", StringType(), True),
        StructField("full_name", StringType(), True)
    ])
    df = test_spark.createDataFrame([("CUST_001", "123456789", "John Doe")], schema)

    mock_rules = {
        "cust_no_not_null": "cust_no IS NOT NULL",
        "national_id_valid_length": "length(national_id) >= 9"
    }

    target_module = "pipeline.silver.customer_validation" if "pipeline.silver.customer_validation" in sys.modules else "customer_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = customer_validation.assess(df, "core_banking_customer")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_captures_rule_failures_and_duplicate_national_ids(test_spark):
    """Verify assess records both constraint failures and duplicate_national_id flags."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("national_id", StringType(), True)
    ])
    data = [
        ("CUST_001", "NAT_UNIQUE"),   # Valid
        (None, "NAT_UNIQUE"),         # Fails cust_no_not_null
        ("CUST_003", "NAT_DUP")       # Triggers duplicate national_id rule
    ]
    df = test_spark.createDataFrame(data, schema)

    # DataFrame chứa danh sách national_id trùng lặp
    dup_schema = StructType([StructField("national_id", StringType(), True)])
    duplicate_ids_df = test_spark.createDataFrame([("NAT_DUP",)], dup_schema)

    mock_rules = {
        "cust_no_not_null": "cust_no IS NOT NULL"
    }

    target_module = "pipeline.silver.customer_validation" if "pipeline.silver.customer_validation" in sys.modules else "customer_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = customer_validation.assess(df, "core_banking_customer", duplicate_ids=duplicate_ids_df)
        rows = {r.cust_no: r for r in res_df.collect()}

        # Row CUST_001: Pass tất cả
        assert len(rows["CUST_001"]._failed_rule_names) == 0

        # Row None (CUST_002): Vi phạm cust_no_not_null
        none_row = next(r for r in res_df.collect() if r.cust_no is None)
        assert none_row._failed_rule_names == ["cust_no_not_null"]

        # Row CUST_003: Vi phạm duplicate national_id
        assert rows["CUST_003"]._failed_rule_names == ["core_banking_customer__national_id__duplicate"]
        assert "_duplicate_national_id" not in res_df.columns  # Đã bị drop đúng như logic


def test_assess_null_evaluations_handled_as_failures(test_spark):
    """Verify constraint expressions evaluating to NULL are coalesced to False and captured."""
    schema = StructType([
        StructField("cust_no", StringType(), True),
        StructField("status", StringType(), True)
    ])
    df = test_spark.createDataFrame([("CUST_001", None)], schema)

    mock_rules = {
        "status_active": "status = 'ACTIVE'"
    }

    target_module = "pipeline.silver.customer_validation" if "pipeline.silver.customer_validation" in sys.modules else "customer_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = customer_validation.assess(df, "core_banking_customer")
        row = res_df.first()

        assert "status_active" in row._failed_rule_names


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])