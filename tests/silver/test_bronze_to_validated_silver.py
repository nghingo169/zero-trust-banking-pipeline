# Databricks notebook source
"""Unit tests for pipeline.silver.bronze_to_validated_silver module.

Tests helper functions, SCD2 version resolution, assessment logic, 
quarantine payload projection, and validation vs. quarantine routing.
"""

from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, ArrayType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & DLT MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT và pyspark.pipelines để chạy độc lập mà không cần DLT Runtime Engine
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

# Mock data_contracts.table_catalog nếu chưa nạp trong môi trường
if "data_contracts.table_catalog" not in sys.modules:
    mock_catalog = MagicMock()
    mock_catalog.DOMAINS = {
        "card": {"scd2": {"card_master": "card_id"}, "append": {}},
        "customer": {"scd2": {"core_banking_customer": "customer_id"}, "append": {}},
        "transaction": {"scd2": {}, "append": {"account_transaction": "account_txn_id"}},
        "fincrime": {"scd2": {}, "append": {"aml_alert": "alert_id"}}
    }
    mock_catalog.tables = lambda domain: {
        "card": {"card_master": "card_id"},
        "customer": {"core_banking_customer": "customer_id"},
        "transaction": {"account_transaction": "account_txn_id"},
        "fincrime": {"aml_alert": "alert_id"}
    }[domain]
    
    sys.modules["data_contracts"] = MagicMock()
    sys.modules["data_contracts.table_catalog"] = mock_catalog


# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE & PRE-IMPORT SPARK CONF
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
            .appName("BronzeToValidatedSilver-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.quality_rules_path", PROJECT_SRC)
            .config("pipeline.catalog", "workspace")
            .config("pipeline.validated_schema", "silver")
            .config("pipeline.governance_schema", "governance")
            .config("pipeline.bronze_schema", "bronze")
            .config("pipeline.run_id", "TEST_RUN_999")
            .getOrCreate()
        )
        import builtins
        builtins.spark = session

    # Đảm bảo các thuộc tính spark.conf tồn tại trước khi module chính nạp
    session.conf.set("pipeline.quality_rules_path", PROJECT_SRC)
    session.conf.set("pipeline.catalog", "workspace")
    session.conf.set("pipeline.validated_schema", "silver")
    session.conf.set("pipeline.governance_schema", "governance")
    session.conf.set("pipeline.bronze_schema", "bronze")

    return session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE (THỰC THI CHÍNH XÁC CÁC SYNTAX TỪ FILE GỐC)
# ------------------------------------------------------------------------------
try:
    from pipeline.silver import bronze_to_validated_silver
except ImportError:
    import bronze_to_validated_silver


# ==============================================================================
# SECTION 1: HELPER FUNCTIONS TESTS
# ==============================================================================

def test_qualified_formatting(test_spark):
    """Verify qualified formats schema and table with global CATALOG."""
    formatted = bronze_to_validated_silver.qualified("silver", "my_table")
    catalog = test_spark.conf.get("pipeline.catalog", "workspace")
    assert formatted == f"{catalog}.silver.my_table"


def test_column_or_null_existing_and_missing(test_spark):
    """Verify _column_or_null casts existing column or returns NULL casted to data_type."""
    schema = StructType([StructField("existing_col", StringType(), True)])
    df = test_spark.createDataFrame([("value_1",)], schema)

    # Test cột đã tồn tại
    col_existing = bronze_to_validated_silver._column_or_null(df, "existing_col", "string")
    res1 = df.select(col_existing.alias("res")).first().res
    assert res1 == "value_1"

    # Test cột không tồn tại
    col_missing = bronze_to_validated_silver._column_or_null(df, "missing_col", "string")
    res2 = df.select(col_missing.alias("res")).first().res
    assert res2 is None


def test_duplicate_active_business_keys(test_spark):
    """Verify duplicate_active_business_keys identifies duplicate active (END_AT IS NULL) keys in SCD2."""
    schema = StructType([
        StructField("card_id", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    data = [
        ("CARD_DUP", None),
        ("CARD_DUP", None),                     # Trùng lặp phiên bản Active
        ("CARD_SINGLE", None),                  # Đơn lẻ Active
        ("CARD_HIST", "2026-01-01T00:00:00")    # Inactive history (không tính trùng)
    ]
    df = test_spark.createDataFrame(data, schema)

    with patch.object(test_spark.read, "table", return_value=df):
        dup_df = bronze_to_validated_silver.duplicate_active_business_keys("card", "card_master", "card_id")
        assert dup_df is not None
        dups = [row.card_id for row in dup_df.collect()]
        assert dups == ["CARD_DUP"]


def test_active_scd2_versions(test_spark):
    """Verify active_scd2_versions extracts start timestamps for current active SCD2 versions."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("__START_AT", StringType(), True),
        StructField("__END_AT", StringType(), True)
    ])
    data = [
        ("CUST_01", "2026-02-01T00:00:00", None),                 # Active version
        ("CUST_01", "2026-01-01T00:00:00", "2026-02-01T00:00:00")  # Historical version
    ]
    df = test_spark.createDataFrame(data, schema)

    with patch.object(test_spark.read, "table", return_value=df):
        active_df = bronze_to_validated_silver.active_scd2_versions("customer", "core_banking_customer", "customer_id")
        assert active_df is not None
        rows = active_df.collect()
        assert len(rows) == 1
        assert rows[0].customer_id == "CUST_01"
        assert rows[0]._active_version_start_at == "2026-02-01T00:00:00"


# ==============================================================================
# SECTION 2: ASSESSMENT LOGIC TESTS
# ==============================================================================

def test_assess_attaches_quarantine_flag_and_json(test_spark):
    """Verify assess normalizes DF, evaluates rule failures & rescued data, and flags is_quarantined."""
    schema = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("_change_type", StringType(), True),
        StructField("_rescued_data", StringType(), True)
    ])
    data = [
        ("TXN_VALID", "2026-07-10", "insert", None),
        ("TXN_INVALID", "2026-07-10", "insert", '{"corrupted": "val"}')
    ]
    df = test_spark.createDataFrame(data, schema)

    # Mock streaming read
    mock_read_stream = MagicMock()
    mock_read_stream.option.return_value.table.return_value.filter.return_value = df

    # Mock module đánh giá quy tắc để giả lập lỗi quy tắc cho TXN_INVALID
    def mock_tx_assess(raw_df, table_name):
        return raw_df.withColumn(
            "_failed_rule_names",
            F.when(F.col("account_txn_id") == "TXN_INVALID", F.array(F.lit("amount_not_null")))
            .otherwise(F.expr("CAST(array() AS ARRAY<STRING>)"))
        )

    target_module = "pipeline.silver.bronze_to_validated_silver" if "pipeline.silver.bronze_to_validated_silver" in sys.modules else "bronze_to_validated_silver"

    with patch.object(test_spark, "readStream", mock_read_stream), \
         patch(f"{target_module}.transaction_validation.assess", side_effect=mock_tx_assess), \
         patch(f"{target_module}.duplicate_active_business_keys", return_value=None):

        assessed_df = bronze_to_validated_silver.assess("transaction", "account_transaction")
        rows = {r.account_txn_id: r for r in assessed_df.collect()}

        # Row hợp lệ
        assert rows["TXN_VALID"].is_quarantined is False
        assert rows["TXN_VALID"].validation_business_date == "2026-07-10"

        # Row bị đưa vào Quarantine (vi phạm rule hoặc chứa rescued_data)
        assert rows["TXN_INVALID"].is_quarantined is True
        assert rows["TXN_INVALID"]._rescued_data_json == '{"corrupted": "val"}'
        assert rows["TXN_INVALID"]._quarantine_payload_json is not None


# ==============================================================================
# SECTION 3: QUARANTINE EVENTS PROJECTION TESTS
# ==============================================================================

def test_quarantine_events_explodes_failed_rules(test_spark):
    """Verify quarantine_events converts quarantined records into atomic DLQ rows per failed rule."""
    schema = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("is_quarantined", BooleanType(), True),
        StructField("_failed_rule_names", ArrayType(StringType()), True),
        StructField("_rescued_data_json", StringType(), True),
        StructField("_quarantine_payload_json", StringType(), True),
        StructField("__START_AT", StringType(), True)
    ])

    data = [
        ("TXN_FAIL", True, ["rule_1", "rule_2"], None, '{"account_txn_id": "TXN_FAIL"}', "2026-01-01T00:00:00")
    ]
    df = test_spark.createDataFrame(data, schema)

    mock_read_stream = MagicMock()
    mock_read_stream.table.return_value = df

    target_module = "pipeline.silver.bronze_to_validated_silver" if "pipeline.silver.bronze_to_validated_silver" in sys.modules else "bronze_to_validated_silver"

    with patch.object(test_spark, "readStream", mock_read_stream), \
         patch(f"{target_module}.active_scd2_versions", return_value=None):

        dlq_df = bronze_to_validated_silver.quarantine_events("transaction", "account_transaction", "account_txn_id")
        rows = dlq_df.collect()

        # Tách thành 2 dòng riêng biệt tương ứng với 2 lỗi rule_1 và rule_2
        assert len(rows) == 2
        rule_names = {r.failed_rule_name for r in rows}
        assert rule_names == {"rule_1", "rule_2"}

        for row in rows:
            assert row.source_table_name == "account_transaction"
            assert row.source_business_key == "TXN_FAIL"
            assert len(row.quarantine_key) == 64  # Chuỗi Hash SHA-256
            assert row.quarantine_reason == "VALIDATION_RULE_FAILURE"


# Entrypoint trực tiếp khi chạy file độc lập
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])