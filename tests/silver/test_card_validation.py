"""Unit tests for pipeline.silver.card_validation module.

Tests Card Validation Logic:
- Evaluation of quality rules returned by get_rules(table_name)
- Correct handling of null condition evaluations (coalesced to False)
- Empty rule registry fallback
- Invocation of schema normalization via data_contracts.normalization
"""

from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT và pyspark.pipelines nếu môi trường chưa có
if "pyspark.pipelines" not in sys.modules:
    from types import ModuleType
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    from types import ModuleType
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

# Mock data_contracts nếu chưa nạp trong môi trường
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
            .appName("CardValidation-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.quality_rules_path", ".")
            .config("pipeline.catalog", "workspace")
            .config("pipeline.validated_schema", "silver")
            .config("pipeline.bronze_schema", "bronze")
            .getOrCreate()
        )
        import builtins
        builtins.spark = session

    return session


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
    """Verify that _failed_rule_names is empty when all card validation rules pass."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("status", StringType(), True)
    ])
    df = test_spark.createDataFrame([("CARD_001", "4532015112830366", "ACTIVE")], schema)

    mock_rules = {
        "card_id_not_null": "card_id IS NOT NULL",
        "card_number_valid_length": "length(card_number) >= 15",
        "status_valid": "status IN ('ACTIVE', 'INACTIVE', 'BLOCKED')"
    }

    target_module = "pipeline.silver.card_validation" if "pipeline.silver.card_validation" in sys.modules else "card_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = card_validation.assess(df, "card_master")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_captures_rule_failures(test_spark):
    """Verify failed rules are correctly recorded in the _failed_rule_names array."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("status", StringType(), True)
    ])
    data = [
        ("CARD_001", "4532015112830366", "ACTIVE"),   # Valid row
        (None, "4532015112830366", "ACTIVE"),         # Fails card_id_not_null
        ("CARD_003", "123", "ACTIVE"),                # Fails card_number_valid_length
        ("CARD_004", "4532015112830366", "INVALID"),   # Fails status_valid
        (None, "123", "UNKNOWN")                      # Fails all 3 rules
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "card_id_not_null": "card_id IS NOT NULL",
        "card_number_valid_length": "length(card_number) >= 15",
        "status_valid": "status IN ('ACTIVE', 'INACTIVE', 'BLOCKED')"
    }

    target_module = "pipeline.silver.card_validation" if "pipeline.silver.card_validation" in sys.modules else "card_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = card_validation.assess(df, "card_master")
        rows = res_df.collect()

        # Row 0: All pass
        assert len(rows[0]._failed_rule_names) == 0

        # Row 1: Fail card_id_not_null
        assert rows[1]._failed_rule_names == ["card_id_not_null"]

        # Row 2: Fail card_number_valid_length
        assert rows[2]._failed_rule_names == ["card_number_valid_length"]

        # Row 3: Fail status_valid
        assert rows[3]._failed_rule_names == ["status_valid"]

        # Row 4: Fail all three
        assert set(rows[4]._failed_rule_names) == {"card_id_not_null", "card_number_valid_length", "status_valid"}


def test_assess_null_expressions_handled_as_failures(test_spark):
    """Verify constraint expressions evaluating to NULL are coalesced to False and captured."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    # Evaluating "status = 'ACTIVE'" on NULL status evaluates to SQL NULL
    df = test_spark.createDataFrame([("CARD_001", None)], schema)

    mock_rules = {
        "status_must_be_active": "status = 'ACTIVE'"
    }

    target_module = "pipeline.silver.card_validation" if "pipeline.silver.card_validation" in sys.modules else "card_validation"

    with patch(f"{target_module}.get_rules", return_value=mock_rules), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = card_validation.assess(df, "card_master")
        row = res_df.first()

        assert "status_must_be_active" in row._failed_rule_names


def test_assess_empty_rules_for_table(test_spark):
    """Verify behavior when no rules are configured for a table."""
    schema = StructType([StructField("card_id", StringType(), True)])
    df = test_spark.createDataFrame([("CARD_001",)], schema)

    target_module = "pipeline.silver.card_validation" if "pipeline.silver.card_validation" in sys.modules else "card_validation"

    with patch(f"{target_module}.get_rules", return_value={}), \
         patch(f"{target_module}.normalize", side_effect=lambda input_df: input_df):

        res_df = card_validation.assess(df, "unregistered_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


def test_assess_triggers_normalization(test_spark):
    """Verify that normalize is explicitly called on the DataFrame."""
    schema = StructType([StructField("card_id", StringType(), True)])
    df = test_spark.createDataFrame([("CARD_001",)], schema)

    mock_normalize = MagicMock(side_effect=lambda input_df: input_df.withColumn("is_normalized", F.lit(True)))

    target_module = "pipeline.silver.card_validation" if "pipeline.silver.card_validation" in sys.modules else "card_validation"

    with patch(f"{target_module}.get_rules", return_value={}), \
         patch(f"{target_module}.normalize", mock_normalize):

        res_df = card_validation.assess(df, "card_master")

        mock_normalize.assert_called_once()
        assert "is_normalized" in res_df.columns


# Entrypoint trực tiếp khi thực thi file
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])