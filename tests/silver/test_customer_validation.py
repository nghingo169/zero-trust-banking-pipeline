# Databricks notebook source
"""Unit tests for pipeline.silver.customer_validation module.

Tests Customer Validation Logic:
- duplicate_national_ids: Detection of duplicate active national IDs
- assess: Schema normalization, quality rule evaluation, duplicate national ID flagging, and _failed_rule_names population
"""

from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT and pyspark.pipelines for local execution
if "pyspark.pipelines" not in sys.modules:
    from types import ModuleType
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda name=None, comment=None: (lambda func: func)
    pipelines_mock.temporary_view = lambda name=None: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    from types import ModuleType
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda name=None, comment=None: (lambda func: func)
    dlt_mock.temporary_view = lambda name=None: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock external contracts if not present in the environment
if "data_contracts.table_catalog" not in sys.modules:
    mock_catalog = MagicMock()
    mock_catalog.DOMAINS = {
        "customer": {
            "scd2": {"core_banking_customer": "customer_id", "crm_customer": "crm_id"},
            "append": {"customer_event": "event_id"}
        }
    }
    sys.modules["data_contracts"] = MagicMock()
    sys.modules["data_contracts.table_catalog"] = mock_catalog

if "data_contracts.normalization" not in sys.modules:
    mock_norm = MagicMock()
    mock_norm.normalize = lambda df: df
    sys.modules["data_contracts.normalization"] = mock_norm

if "data_contracts.quality_rules.registry" not in sys.modules:
    mock_registry = MagicMock()
    mock_registry.get_rules = lambda table_name: {}
    sys.modules["data_contracts.quality_rules.registry"] = mock_registry

# ------------------------------------------------------------------------------
# 2. LOCAL SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    """Provides local SparkSession or uses active Databricks Spark environment."""
    try:
        return spark  # type: ignore # noqa: F821
    except NameError:
        spark_session = (
            SparkSession.builder
            .master("local[1]")
            .appName("CustomerValidation-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        import builtins
        builtins.spark = spark_session
        return spark_session

# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
try:
    from pipeline.silver import customer_validation
except ImportError:
    import customer_validation


# ==============================================================================
# SECTION 1: DUPLICATE NATIONAL IDS TESTS
# ==============================================================================

def test_duplicate_national_ids_non_identity_table(test_spark):
    """Verify return is None for tables not in IDENTITY_TABLES."""
    result = customer_validation.duplicate_national_ids(test_spark, "workspace.bronze.customer_event")
    assert result is None


def test_duplicate_national_ids_detection(test_spark):
    """Verify duplicate active national IDs are identified correctly."""
    schema = StructType([
        StructField("national_id", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    data = [
        ("NID_100", None),
        ("NID_100", None),                          # Duplicate active NID_100
        ("NID_200", None),                          # Single active NID_200
        ("NID_300", None),
        ("NID_300", "2026-01-01T00:00:00.000Z")     # Inactive version (should not count as duplicate)
    ]
    df = test_spark.createDataFrame(data, schema)

    with patch.object(test_spark.read, "table", return_value=df), \
         patch.object(customer_validation, "normalize", side_effect=lambda input_df: input_df):
        
        dup_df = customer_validation.duplicate_national_ids(test_spark, "workspace.bronze.core_banking_customer")
        
        assert dup_df is not None
        dups = [row.national_id for row in dup_df.collect()]
        assert dups == ["NID_100"]


# ==============================================================================
# SECTION 2: CUSTOMER ASSESS TESTS
# ==============================================================================

def test_assess_basic_quality_rules(test_spark):
    """Verify basic rules pass/fail correctly without duplicate ID checks."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("national_id", StringType(), True)
    ])
    data = [
        ("CUST_001", "NID_100"),  # Valid
        (None, "NID_200")         # Fails customer_id_not_null
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "customer_id_not_null": "customer_id IS NOT NULL"
    }

    with patch.object(customer_validation, "get_rules", return_value=mock_rules), \
         patch.object(customer_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = customer_validation.assess(df, "core_banking_customer")
        rows = res_df.collect()

        # Row 1 passes
        assert len(rows[0]._failed_rule_names) == 0

        # Row 2 fails rule
        assert rows[1]._failed_rule_names == ["customer_id_not_null"]


def test_assess_with_duplicate_national_ids(test_spark):
    """Verify duplicate national ID failure is appended to _failed_rule_names."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("national_id", StringType(), True)
    ])
    df = test_spark.createDataFrame([
        ("CUST_001", "NID_DUP"),
        ("CUST_002", "NID_UNIQUE")
    ], schema)

    dup_df = test_spark.createDataFrame([("NID_DUP",)], ["national_id"])

    mock_rules = {
        "customer_id_not_null": "customer_id IS NOT NULL"
    }

    with patch.object(customer_validation, "get_rules", return_value=mock_rules), \
         patch.object(customer_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = customer_validation.assess(df, "core_banking_customer", duplicate_ids=dup_df)
        rows = {row.customer_id: row for row in res_df.collect()}

        # CUST_001 should have duplicate rule failure appended
        assert "core_banking_customer__national_id__duplicate" in rows["CUST_001"]._failed_rule_names
        
        # CUST_002 should pass clean
        assert len(rows["CUST_002"]._failed_rule_names) == 0
        
        # Ensure temporary helper column is dropped
        assert "_duplicate_national_id" not in res_df.columns


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])