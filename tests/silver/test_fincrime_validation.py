# Databricks notebook source
"""Unit tests for pipeline.silver.fincrime_validation module.

Tests Financial Crime Validation Logic:
- Evaluation of quality rules defined in RULES_BY_TABLE dictionary structure
- Correct handling of null condition evaluations (coalesced to False)
- Fallback behavior when no rules are configured for a table
- Schema normalization invocation via data_contracts.normalization
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
        "fincrime": {
            "scd2": {"sanction_watchlist": "watchlist_id"},
            "append": {"aml_alert": "alert_id"}
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
    mock_registry.RULES_BY_TABLE = {}
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
            .appName("FincrimeValidation-UnitTest")
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
    from pipeline.silver import fincrime_validation
except ImportError:
    import fincrime_validation


# ==============================================================================
# SECTION: FINCRIME VALIDATION ASSESS TESTS
# ==============================================================================

def test_assess_all_rules_passing(test_spark):
    """Verify that _failed_rule_names is empty when all constraints pass."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("risk_score", DoubleType(), True)
    ])
    df = test_spark.createDataFrame([("ALT_001", 85.5)], schema)

    mock_rules = [
        {"name": "alert_id_not_null", "constraint": "alert_id IS NOT NULL"},
        {"name": "valid_risk_score", "constraint": "risk_score >= 0.0 AND risk_score <= 100.0"}
    ]

    with patch.dict(fincrime_validation.RULES_BY_TABLE, {"aml_alert": mock_rules}, clear=True), \
         patch.object(fincrime_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = fincrime_validation.assess(df, "aml_alert")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_captures_rule_failures(test_spark):
    """Verify failing constraints append their rule name into _failed_rule_names."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("risk_score", DoubleType(), True)
    ])
    data = [
        ("ALT_001", 90.0),    # Valid
        (None, 50.0),         # Fails alert_id_not_null
        ("ALT_003", 150.0),   # Fails valid_risk_score
        (None, -10.0)         # Fails both
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = [
        {"name": "alert_id_not_null", "constraint": "alert_id IS NOT NULL"},
        {"name": "valid_risk_score", "constraint": "risk_score >= 0.0 AND risk_score <= 100.0"}
    ]

    with patch.dict(fincrime_validation.RULES_BY_TABLE, {"aml_alert": mock_rules}, clear=True), \
         patch.object(fincrime_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = fincrime_validation.assess(df, "aml_alert")
        rows = res_df.collect()

        # Row 0: Clean
        assert len(rows[0]._failed_rule_names) == 0

        # Row 1: Fail alert_id_not_null
        assert rows[1]._failed_rule_names == ["alert_id_not_null"]

        # Row 2: Fail valid_risk_score
        assert rows[2]._failed_rule_names == ["valid_risk_score"]

        # Row 3: Fail both
        assert set(rows[3]._failed_rule_names) == {"alert_id_not_null", "valid_risk_score"}


def test_assess_null_constraints_handled_as_failures(test_spark):
    """Verify constraint expressions resulting in NULL are coalesced to False and reported."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    # Evaluating "status = 'OPEN'" on NULL status evaluates to NULL in SQL
    df = test_spark.createDataFrame([("ALT_001", None)], schema)

    mock_rules = [
        {"name": "status_is_open", "constraint": "status = 'OPEN'"}
    ]

    with patch.dict(fincrime_validation.RULES_BY_TABLE, {"aml_alert": mock_rules}, clear=True), \
         patch.object(fincrime_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = fincrime_validation.assess(df, "aml_alert")
        row = res_df.first()

        assert "status_is_open" in row._failed_rule_names


def test_assess_empty_rules_table(test_spark):
    """Verify behavior when table has no configured rules in RULES_BY_TABLE."""
    schema = StructType([StructField("alert_id", StringType(), True)])
    df = test_spark.createDataFrame([("ALT_001",)], schema)

    with patch.dict(fincrime_validation.RULES_BY_TABLE, {}, clear=True), \
         patch.object(fincrime_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = fincrime_validation.assess(df, "unregistered_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])