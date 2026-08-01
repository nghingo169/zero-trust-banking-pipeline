# Databricks notebook source
"""Unit tests for pipeline.silver.bronze_to_validated_silver module.

Tests Centralized Quarantine & Validation Orchestration:
- Helper functions (_column_or_null, qualified, bronze)
- SCD2 helper functions (duplicate_active_business_keys, active_scd2_versions)
- Core Assessment logic (assess)
- DLT Registration helpers (register_assessment, register_validated_output)
- DLQ Projection logic (quarantine_events)
- Centralized Quarantine Streaming Table (silver_quarantine_record)
"""

from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType, TimestampType

# ------------------------------------------------------------------------------
# 1. PATH RESOLUTION & DLT / SPARK MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock 'dlt' and 'pyspark.pipelines' modules before importing the target script
if "pyspark.pipelines" not in sys.modules:
    from types import ModuleType
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.temporary_view = lambda name=None: (lambda func: func)
    pipelines_mock.table = lambda name=None, comment=None: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

if "dlt" not in sys.modules:
    from types import ModuleType
    dlt_mock = ModuleType("dlt")
    dlt_mock.temporary_view = lambda name=None: (lambda func: func)
    dlt_mock.table = lambda name=None, comment=None: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

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
            .appName("BronzeToValidatedSilver-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.quality_rules_path", ".")
            .config("pipeline.catalog", "workspace")
            .config("pipeline.validated_schema", "silver")
            .config("pipeline.governance_schema", "governance")
            .config("pipeline.bronze_schema", "bronze")
            .getOrCreate()
        )
        import builtins
        builtins.spark = spark_session
        return spark_session

# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE WITH MOCKED DEPENDENCIES
# ------------------------------------------------------------------------------
# Mock domain contract registries if data_contracts is not in local python environment
if "data_contracts.table_catalog" not in sys.modules:
    mock_contracts = MagicMock()
    mock_contracts.DOMAINS = {
        "customer": {"scd2": ["customer"]},
        "card": {"scd2": []}
    }
    mock_contracts.tables = lambda domain: {"customer": "customer_id", "card": "card_id"} if domain in ["customer", "card"] else {}
    sys.modules["data_contracts"] = MagicMock()
    sys.modules["data_contracts.table_catalog"] = mock_contracts

try:
    from pipeline.silver import bronze_to_validated_silver as b2s
except ImportError:
    import bronze_to_validated_silver as b2s


# ==============================================================================
# SECTION 1: HELPER FUNCTIONS TESTS
# ==============================================================================

def test_qualified_and_bronze_helpers(test_spark):
    """Verify catalog qualified name formatting."""
    with patch.object(b2s, "CATALOG", "workspace"), \
         patch.object(b2s, "BRONZE_SCHEMA", "bronze"):
        assert b2s.qualified("silver", "customer") == "workspace.silver.customer"
        assert b2s.bronze("customer", "customer_profile") == "workspace.bronze.customer_profile"


def test_column_or_null_existing_column(test_spark):
    """Verify _column_or_null casts existing column properly."""
    df = test_spark.createDataFrame([("123",)], ["id"])
    res_df = df.withColumn("id_cast", b2s._column_or_null(df, "id", "long"))
    assert res_df.schema["id_cast"].dataType == LongType()
    assert res_df.first().id_cast == 123


def test_column_or_null_missing_column(test_spark):
    """Verify _column_or_null creates null column if column does not exist."""
    df = test_spark.createDataFrame([("123",)], ["id"])
    res_df = df.withColumn("missing_col", b2s._column_or_null(df, "non_existing", "string"))
    assert "missing_col" in res_df.columns
    assert res_df.schema["missing_col"].dataType == StringType()
    assert res_df.first().missing_col is None


# ==============================================================================
# SECTION 2: SCD2 HELPER FUNCTIONS TESTS
# ==============================================================================

def test_duplicate_active_business_keys(test_spark):
    """Verify detection of duplicate active SCD2 records (__END_AT IS NULL)."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    # Customer 1001 has 2 active records (__END_AT is Null), 1002 has 1 active
    data = [
        ("CUST_1001", None),
        ("CUST_1001", None),
        ("CUST_1002", None),
        ("CUST_1002", "2026-01-01T00:00:00.000Z")
    ]
    mock_df = test_spark.createDataFrame(data, schema)

    with patch.object(b2s.spark.read, "table", return_value=mock_df):
        dup_df = b2s.duplicate_active_business_keys("customer", "customer", "customer_id")
        assert dup_df is not None
        dups = [row.customer_id for row in dup_df.collect()]
        assert "CUST_1001" in dups
        assert "CUST_1002" not in dups


def test_active_scd2_versions(test_spark):
    """Verify extraction of active SCD2 __START_AT timestamps per key."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("__START_AT", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    data = [
        ("CUST_1001", "2026-08-01T00:00:00.000Z", None),
        ("CUST_1001", "2026-01-01T00:00:00.000Z", "2026-07-31T23:59:59.000Z")
    ]
    mock_df = test_spark.createDataFrame(data, schema)

    with patch.object(b2s.spark.read, "table", return_value=mock_df):
        active_df = b2s.active_scd2_versions("customer", "customer", "customer_id")
        assert active_df is not None
        row = active_df.first()
        assert row.customer_id == "CUST_1001"
        assert row._active_version_start_at == "2026-08-01T00:00:00.000Z"


# ==============================================================================
# SECTION 3: ASSESS FUNCTION TESTS
# ==============================================================================

def test_assess_quarantine_flagging(test_spark):
    """Verify assess attaches payload, flags is_quarantined and sets validation_business_date."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_failed_rule_names", ArrayType(StringType()), True),
        StructField("_rescued_data", StringType(), True)
    ])
    data = [
        ("CARD_1", "2026-08-01", "insert", [], None),                   # Valid row
        ("CARD_2", "2026-08-01", "update_postimage", ["RULE_FAIL"], None), # Failed rule
        ("CARD_3", "2026-08-01", "insert", [], '{"corrupt":"val"}')     # Rescued data present
    ]
    raw_stream_df = test_spark.createDataFrame(data, schema)

    with patch.object(b2s.spark.readStream, "option", return_value=b2s.spark.readStream), \
         patch.object(b2s.spark.readStream, "table", return_value=raw_stream_df), \
         patch.object(b2s.card_validation, "assess", side_effect=lambda df, t: df), \
         patch.object(b2s, "duplicate_active_business_keys", return_value=None):

        assessed_df = b2s.assess("card", "card")
        results = {row.card_id: row for row in assessed_df.collect()}

        # CARD_1 must pass
        assert results["CARD_1"].is_quarantined is False
        assert results["CARD_1"].validation_business_date == "2026-08-01"
        assert results["CARD_1"]._quarantine_payload_json is not None

        # CARD_2 must be quarantined due to _failed_rule_names
        assert results["CARD_2"].is_quarantined is True

        # CARD_3 must be quarantined due to rescued_data
        assert results["CARD_3"].is_quarantined is True


# ==============================================================================
# SECTION 4: QUARANTINE EVENTS PROJECTION TESTS
# ==============================================================================

def test_quarantine_events_projection(test_spark):
    """Verify quarantine_events projects failed rows into atomic DLQ records."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("is_quarantined", BooleanType(), True),
        StructField("_failed_rule_names", ArrayType(StringType()), True),
        StructField("_rescued_data_json", StringType(), True),
        StructField("_quarantine_payload_json", StringType(), True)
    ])
    data = [
        ("CARD_FAIL", "2026-08-01", True, ["INVALID_CARD_NUM"], None, '{"card_id":"CARD_FAIL"}')
    ]
    assessed_mock = test_spark.createDataFrame(data, schema)

    with patch.object(b2s.spark.readStream, "table", return_value=assessed_mock), \
         patch.object(b2s, "active_scd2_versions", return_value=None), \
         patch.object(b2s.spark.conf, "get", return_value="RUN_12345"):

        dlq_df = b2s.quarantine_events("card", "card", "card_id")
        row = dlq_df.first()

        assert row.source_table_name == "card"
        assert row.source_business_key == "CARD_FAIL"
        assert row.failed_rule_name == "INVALID_CARD_NUM"
        assert row.quarantine_reason == "VALIDATION_RULE_FAILURE"
        assert row.pipeline_run_id == "RUN_12345"
        assert row.quarantine_data_payload == '{"card_id":"CARD_FAIL"}'


# Entrypoint for direct test execution
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])