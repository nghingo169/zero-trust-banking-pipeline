# Databricks notebook source
"""Unit tests for pipeline.silver.card_validation module.

Tests Card Validation Assessment Logic:
- Schema normalization invocation via data_contracts.normalization
- Quality rule evaluation (passing vs failing rules)
- Null-handling in quality rule evaluation
- Population of _failed_rule_names column
"""

from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT and pyspark.pipelines for local testing
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
        "card": {
            "scd2": {"card_master": "card_id"},
            "append": {"card_transaction": "card_txn_id"}
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
            .appName("CardValidation-UnitTest")
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
    from pipeline.silver import card_validation
except ImportError:
    import card_validation


# ==============================================================================
# SECTION: CARD VALIDATION ASSESS TESTS
# ==============================================================================

def test_assess_all_rules_passing(test_spark):
    """Verify that _failed_rule_names is empty when all quality rules pass."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("credit_limit", DoubleType(), True)
    ])
    data = [("CARD_001", 5000.0)]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "card_id_not_null": "card_id IS NOT NULL",
        "valid_credit_limit": "credit_limit > 0"
    }

    with patch.object(card_validation, "get_rules", return_value=mock_rules), \
         patch.object(card_validation, "normalize", side_effect=lambda df: df):
        
        res_df = card_validation.assess(df, "card_master")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_single_and_multiple_rule_failures(test_spark):
    """Verify failed rule names are correctly captured into _failed_rule_names array."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("credit_limit", DoubleType(), True)
    ])
    data = [
        ("CARD_VALID", 1000.0),   # Pass both
        (None, 2000.0),           # Fail card_id_not_null
        ("CARD_INVALID", -100.0), # Fail valid_credit_limit
        (None, -500.0)            # Fail both
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "card_id_not_null": "card_id IS NOT NULL",
        "valid_credit_limit": "credit_limit > 0"
    }

    with patch.object(card_validation, "get_rules", return_value=mock_rules), \
         patch.object(card_validation, "normalize", side_effect=lambda df: df):
        
        res_df = card_validation.assess(df, "card_master")
        rows = res_df.collect()

        # Row 1: Valid
        assert len(rows[0]._failed_rule_names) == 0

        # Row 2: Failed card_id_not_null
        assert rows[1]._failed_rule_names == ["card_id_not_null"]

        # Row 3: Failed valid_credit_limit
        assert rows[2]._failed_rule_names == ["valid_credit_limit"]

        # Row 4: Failed both
        assert set(rows[3]._failed_rule_names) == {"card_id_not_null", "valid_credit_limit"}


def test_assess_null_evaluations_handled_as_failures(test_spark):
    """Verify rule evaluations resulting in NULL are treated as failures (coalesced to False)."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    # Comparing null status with 'ACTIVE' produces NULL in SQL logic
    data = [("CARD_001", None)]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "status_is_active": "status = 'ACTIVE'"
    }

    with patch.object(card_validation, "get_rules", return_value=mock_rules), \
         patch.object(card_validation, "normalize", side_effect=lambda df: df):
        
        res_df = card_validation.assess(df, "card_master")
        row = res_df.first()

        assert "status_is_active" in row._failed_rule_names


def test_assess_triggers_normalization(test_spark):
    """Verify assess calls normalize function on input DataFrame."""
    schema = StructType([StructField("card_id", StringType(), True)])
    df = test_spark.createDataFrame([("CARD_001",)], schema)

    mock_normalize = MagicMock(side_effect=lambda input_df: input_df.withColumn("normalized", F.lit(True)))

    with patch.object(card_validation, "get_rules", return_value={}), \
         patch.object(card_validation, "normalize", mock_normalize):
        
        res_df = card_validation.assess(df, "card_master")
        
        mock_normalize.assert_called_once()
        assert "normalized" in res_df.columns


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])