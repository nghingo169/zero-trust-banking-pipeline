"""Unit tests for pipeline.silver.transaction_validation module.

Tests Transaction Validation Logic:
- Evaluation of quality rules returned by RULES_BY_TABLE[table_name]
- Correct handling of null condition evaluations (coalesced to False)
- Empty/Missing rule dictionary fallback (returns empty array<string>)
- Invocation of schema normalization via data_contracts.normalization
"""

import builtins
import os
import sys

# Databricks notebook source
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
        SparkSession.builder.master("local[1]")
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

# Mock đầy đủ 4 domains để tránh KeyError trong validation modules


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import transaction_validation

# ==============================================================================
# SECTION: TRANSACTION VALIDATION ASSESS TESTS
# ==============================================================================


def test_assess_all_rules_passing(test_spark):
    """Verify that _failed_rule_names is empty when all transaction validation rules pass."""
    schema = StructType(
        [
            StructField("account_txn_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("direction", StringType(), True),
        ]
    )
    df = test_spark.createDataFrame([("TXN_001", 500.0, "DEBIT")], schema)

    mock_rules = [
        {"name": "txn_id_not_null", "constraint": "account_txn_id IS NOT NULL"},
        {"name": "amount_positive", "constraint": "amount > 0"},
        {"name": "direction_valid", "constraint": "direction IN ('CREDIT', 'DEBIT')"},
    ]

    target_module = (
        "pipeline.silver.transaction_validation"
        if "pipeline.silver.transaction_validation" in sys.modules
        else "transaction_validation"
    )

    with patch.dict(
        f"{target_module}.RULES_BY_TABLE",
        {"account_transaction": mock_rules},
        clear=True,
    ), patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = transaction_validation.assess(df, "account_transaction")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_captures_rule_failures(test_spark):
    """Verify failed rules are correctly recorded in the _failed_rule_names array."""
    schema = StructType(
        [
            StructField("account_txn_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("direction", StringType(), True),
        ]
    )
    data = [
        ("TXN_001", 100.0, "DEBIT"),  # Valid row
        (None, 100.0, "DEBIT"),  # Fails txn_id_not_null
        ("TXN_003", -50.0, "DEBIT"),  # Fails amount_positive
        ("TXN_004", 100.0, "UNKNOWN"),  # Fails direction_valid
        (None, -10.0, "INVALID"),  # Fails all 3 rules
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = [
        {"name": "txn_id_not_null", "constraint": "account_txn_id IS NOT NULL"},
        {"name": "amount_positive", "constraint": "amount > 0"},
        {"name": "direction_valid", "constraint": "direction IN ('CREDIT', 'DEBIT')"},
    ]

    target_module = (
        "pipeline.silver.transaction_validation"
        if "pipeline.silver.transaction_validation" in sys.modules
        else "transaction_validation"
    )

    with patch.dict(
        f"{target_module}.RULES_BY_TABLE",
        {"account_transaction": mock_rules},
        clear=True,
    ), patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = transaction_validation.assess(df, "account_transaction")
        rows = res_df.collect()

        # Row 0: All pass
        assert len(rows[0]._failed_rule_names) == 0

        # Row 1: Fail txn_id_not_null
        assert rows[1]._failed_rule_names == ["txn_id_not_null"]

        # Row 2: Fail amount_positive
        assert rows[2]._failed_rule_names == ["amount_positive"]

        # Row 3: Fail direction_valid
        assert rows[3]._failed_rule_names == ["direction_valid"]

        # Row 4: Fail all three
        assert set(rows[4]._failed_rule_names) == {
            "txn_id_not_null",
            "amount_positive",
            "direction_valid",
        }


def test_assess_null_expressions_handled_as_failures(test_spark):
    """Verify constraint expressions evaluating to NULL are coalesced to False and captured."""
    schema = StructType(
        [
            StructField("account_txn_id", StringType(), True),
            StructField("direction", StringType(), True),
        ]
    )
    # Evaluating "direction = 'DEBIT'" on NULL direction evaluates to SQL NULL
    df = test_spark.createDataFrame([("TXN_001", None)], schema)

    mock_rules = [
        {"name": "direction_must_be_debit", "constraint": "direction = 'DEBIT'"}
    ]

    target_module = (
        "pipeline.silver.transaction_validation"
        if "pipeline.silver.transaction_validation" in sys.modules
        else "transaction_validation"
    )

    with patch.dict(
        f"{target_module}.RULES_BY_TABLE",
        {"account_transaction": mock_rules},
        clear=True,
    ), patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = transaction_validation.assess(df, "account_transaction")
        row = res_df.first()

        assert "direction_must_be_debit" in row._failed_rule_names


def test_assess_empty_rules_for_table(test_spark):
    """Verify behavior when no rules are configured for a table (returns empty array)."""
    schema = StructType([StructField("account_txn_id", StringType(), True)])
    df = test_spark.createDataFrame([("TXN_001",)], schema)

    target_module = (
        "pipeline.silver.transaction_validation"
        if "pipeline.silver.transaction_validation" in sys.modules
        else "transaction_validation"
    )

    with patch.dict(f"{target_module}.RULES_BY_TABLE", {}, clear=True), patch(
        f"{target_module}.normalize", side_effect=lambda input_df: input_df
    ):

        res_df = transaction_validation.assess(df, "unregistered_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_triggers_normalization(test_spark):
    """Verify that normalize is explicitly called on the DataFrame."""
    schema = StructType([StructField("account_txn_id", StringType(), True)])
    df = test_spark.createDataFrame([("TXN_001",)], schema)

    mock_normalize = MagicMock(
        side_effect=lambda input_df: input_df.withColumn("is_normalized", F.lit(True))
    )

    target_module = (
        "pipeline.silver.transaction_validation"
        if "pipeline.silver.transaction_validation" in sys.modules
        else "transaction_validation"
    )

    with patch.dict(f"{target_module}.RULES_BY_TABLE", {}, clear=True), patch(
        f"{target_module}.normalize", mock_normalize
    ):

        res_df = transaction_validation.assess(df, "account_transaction")

        mock_normalize.assert_called_once()
        assert "is_normalized" in res_df.columns


# Entrypoint trực tiếp khi thực thi file
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
