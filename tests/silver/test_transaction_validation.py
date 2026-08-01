# Databricks notebook source
"""Unit tests for pipeline.silver.transaction_validation module.

Tests Transaction Validation Logic:
- Schema normalization invocation via data_contracts.normalization
- Quality rule evaluation based on RULES_BY_TABLE dictionary registry
- Handling of null constraint outcomes (coalesced to False)
- Fallback array handling when no rules exist for a transaction table
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

# Mock external data contracts if not present in the runtime environment
if "data_contracts.table_catalog" not in sys.modules:
    mock_catalog = MagicMock()
    mock_catalog.DOMAINS = {
        "transaction": {
            "scd2": {"account_transaction": "account_txn_id"},
            "append": {"payment_gateway_event": "event_id"}
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
            .appName("TransactionValidation-UnitTest")
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
    from pipeline.silver import transaction_validation
except ImportError:
    import transaction_validation


# ==============================================================================
# SECTION: TRANSACTION VALIDATION ASSESS TESTS
# ==============================================================================

def test_assess_all_rules_passing(test_spark):
    """Verify that _failed_rule_names is empty when all transaction constraints pass."""
    schema = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
    df = test_spark.createDataFrame([("TXN_1001", 150.00, "AUD")], schema)

    mock_rules = [
        {"name": "txn_id_not_null", "constraint": "account_txn_id IS NOT NULL"},
        {"name": "positive_amount", "constraint": "amount > 0.0"},
        {"name": "valid_currency", "constraint": "currency IN ('AUD', 'USD', 'NZD')"}
    ]

    with patch.dict(transaction_validation.RULES_BY_TABLE, {"account_transaction": mock_rules}, clear=True), \
         patch.object(transaction_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = transaction_validation.assess(df, "account_transaction")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_captures_failing_rules(test_spark):
    """Verify failed constraint rule names are appended into _failed_rule_names."""
    schema = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
    data = [
        ("TXN_1001", 250.0, "AUD"),   # Pass all
        (None, 100.0, "AUD"),         # Fail txn_id_not_null
        ("TXN_1003", -50.0, "AUD"),   # Fail positive_amount
        ("TXN_1004", 500.0, "EUR"),   # Fail valid_currency
        (None, -10.0, "JPY")          # Fail all three
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = [
        {"name": "txn_id_not_null", "constraint": "account_txn_id IS NOT NULL"},
        {"name": "positive_amount", "constraint": "amount > 0.0"},
        {"name": "valid_currency", "constraint": "currency IN ('AUD', 'USD', 'NZD')"}
    ]

    with patch.dict(transaction_validation.RULES_BY_TABLE, {"account_transaction": mock_rules}, clear=True), \
         patch.object(transaction_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = transaction_validation.assess(df, "account_transaction")
        rows = res_df.collect()

        # Row 0: Clean
        assert len(rows[0]._failed_rule_names) == 0

        # Row 1: Fail txn_id_not_null
        assert rows[1]._failed_rule_names == ["txn_id_not_null"]

        # Row 2: Fail positive_amount
        assert rows[2]._failed_rule_names == ["positive_amount"]

        # Row 3: Fail valid_currency
        assert rows[3]._failed_rule_names == ["valid_currency"]

        # Row 4: Fail all three
        assert set(rows[4]._failed_rule_names) == {"txn_id_not_null", "positive_amount", "valid_currency"}


def test_assess_null_evaluations_handled_as_failures(test_spark):
    """Verify constraint expressions resulting in NULL are coalesced to False."""
    schema = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("merchant_category", StringType(), True)
    ])
    df = test_spark.createDataFrame([("TXN_1001", None)], schema)

    mock_rules = [
        {"name": "valid_merchant_cat", "constraint": "merchant_category = 'RETAIL'"}
    ]

    with patch.dict(transaction_validation.RULES_BY_TABLE, {"account_transaction": mock_rules}, clear=True), \
         patch.object(transaction_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = transaction_validation.assess(df, "account_transaction")
        row = res_df.first()

        assert "valid_merchant_cat" in row._failed_rule_names


def test_assess_empty_rules_for_table(test_spark):
    """Verify behavior when no rules are configured for a given table name."""
    schema = StructType([StructField("account_txn_id", StringType(), True)])
    df = test_spark.createDataFrame([("TXN_1001",)], schema)

    with patch.dict(transaction_validation.RULES_BY_TABLE, {}, clear=True), \
         patch.object(transaction_validation, "normalize", side_effect=lambda input_df: input_df):
        
        res_df = transaction_validation.assess(df, "unregistered_transaction_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_triggers_normalization(test_spark):
    """Verify that normalize is explicitly called on the DataFrame."""
    schema = StructType([StructField("account_txn_id", StringType(), True)])
    df = test_spark.createDataFrame([("TXN_1001",)], schema)

    mock_normalize = MagicMock(side_effect=lambda input_df: input_df.withColumn("is_normalized", F.lit(True)))

    with patch.dict(transaction_validation.RULES_BY_TABLE, {}, clear=True), \
         patch.object(transaction_validation, "normalize", mock_normalize):
        
        res_df = transaction_validation.assess(df, "account_transaction")
        
        mock_normalize.assert_called_once()
        assert "is_normalized" in res_df.columns


# Entrypoint for direct execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])