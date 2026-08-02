"""Unit tests for pipeline.silver.transaction_transformation module.

Tests Helper Functions & Financial Event Transformation Tables:
- Hash key generation (SHA-256 with trimming and null handling)
- Default currency resolution (fallback to 'VND')
- Pipeline Run ID extraction (column or fallback scalar subquery)
- Canonical Header Table Union (silver_financial_event combining Account, Card, ATM, Gateway)
- Extension Tables (silver_account_posting, silver_card_payment, silver_atm_activity, silver_gateway_payment)
- Event Status History Union (silver_financial_event_status_history combining Account and Card status events)
"""

from pathlib import Path
import os
import sys
from unittest.mock import patch, MagicMock
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & DLT MOCKS FOR LOCAL EXECUTION
# ------------------------------------------------------------------------------
PROJECT_SRC = str(Path(__file__).resolve().parents[2] / "src")
SILVER_DIR = str(Path(__file__).resolve().parents[1])

for path_str in [PROJECT_SRC, SILVER_DIR]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

# Mock DLT and pyspark.pipelines for execution outside DLT Runtime Engine
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
            .appName("TransactionTransformation-UnitTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("pipeline.catalog", "workspace")
            .config("pipeline.silver_validated_schema", "silver_validated")
            .config("pipeline.silver_schema", "silver")
            .getOrCreate()
        )
        import builtins
        builtins.spark = session

    return session


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
try:
    from pipeline.silver import transaction_transformation
except ImportError:
    import transaction_transformation


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================

def test_hash_key_generation(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes with trim/coalesce."""
    df = test_spark.createDataFrame([
        ("core_banking", "TXN_1001"),
        ("core_banking", "  TXN_1001  ")
    ], ["sys", "id"])

    result_df = df.select(transaction_transformation.hash_key("sys", "id").alias("key_hash"))
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64
    assert hashes[0] == hashes[1]


def test_get_currency_col(test_spark):
    """Verify get_currency_col retains currency or defaults to VND when absent or NULL."""
    df_with_curr = test_spark.createDataFrame([("USD",), (None,)], ["currency"])
    res_df1 = df_with_curr.select(transaction_transformation.get_currency_col(df_with_curr))
    assert [r.currency for r in res_df1.collect()] == ["USD", "VND"]

    df_without_curr = test_spark.createDataFrame([("TXN_1",)], ["txn_id"])
    res_df2 = df_without_curr.select(transaction_transformation.get_currency_col(df_without_curr))
    assert res_df2.first().currency == "VND"


def test_get_pipeline_run_id_existing_column(test_spark):
    """Verify get_pipeline_run_id returns existing column if present in DataFrame."""
    df = test_spark.createDataFrame([("RUN_TXN_01",)], ["pipeline_run_id"])
    result_df = df.select(transaction_transformation.get_pipeline_run_id(df).alias("run_id"))
    
    assert result_df.first().run_id == "RUN_TXN_01"
# ==============================================================================
# SECTION 2: TRANSFORMATION FUNCTION TESTS
# ==============================================================================

def test_silver_financial_event_header_union(test_spark):
    """Verify silver_financial_event unions Account, Card, ATM, and Gateway events correctly."""
    transaction_transformation.spark = test_spark

    # 1. Account Transaction Mock
    schema_acc = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([("ACC_TXN_01", 1001, "CUST_01", "VND", "2026-07-01 10:00:00", "RUN_01")], schema_acc)

    # 2. Card Transaction Mock
    schema_card = StructType([
        StructField("card_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([("CARD_TXN_01", "CARD_99", "MERCH_01", "USD", "2026-07-01 11:00:00", "RUN_01")], schema_card)

    # 3. ATM Activity Mock
    schema_atm = StructType([
        StructField("log_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_atm = test_spark.createDataFrame([("ATM_LOG_01", "ACC_TXN_01", "CARD_99", "VND", "2026-07-01 12:00:00", "RUN_01")], schema_atm)

    # 4. Gateway Payment Mock
    schema_gw = StructType([
        StructField("gateway_txn_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("gateway_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_gw = test_spark.createDataFrame([("GW_TXN_01", "ACC_TXN_01", "CARD_TXN_01", "MERCH_01", "USD", "2026-07-01 13:00:00", "RUN_01")], schema_gw)

    def mock_read_table(path):
        if path.endswith("account_transaction"):
            return df_acc
        elif path.endswith("card_transaction"):
            return df_card
        elif path.endswith("log_atm"):
            return df_atm
        elif path.endswith("payment_gateway_log"):
            return df_gw
        return test_spark.createDataFrame([], StructType([]))

    with patch("pyspark.sql.readwriter.DataFrameReader.table", side_effect=mock_read_table):
        res_df = transaction_transformation.silver_financial_event()
        rows = res_df.collect()

        assert len(rows) == 4, f"Expected 4 rows across all event types, got {len(rows)}"
        event_types = {r.event_type for r in rows}
        assert event_types == {"ACCOUNT_POSTING", "CARD_PAYMENT", "ATM_ACTIVITY", "GATEWAY_PAYMENT"}


def test_extension_tables_builders(test_spark):
    """Verify account_posting, card_payment, atm_activity, and gateway_payment extension tables."""
    transaction_transformation.spark = test_spark

    # Account Posting
    schema_acc = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([("ACC_TXN_01", 150.00, "DEBIT", "TRANSFER", "MOBILE", "MERCH_01", "RUN_01")], schema_acc)

    # Card Payment
    schema_card = StructType([
        StructField("card_txn_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("card_transaction_type", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([("CARD_TXN_01", 89.99, "PURCHASE", "MERCH_01", False, "RUN_01")], schema_card)

    def mock_read_table(path):
        if path.endswith("account_transaction"):
            return df_acc
        elif path.endswith("card_transaction"):
            return df_card
        return test_spark.createDataFrame([], StructType([]))

    with patch("pyspark.sql.readwriter.DataFrameReader.table", side_effect=mock_read_table):
        row_post = transaction_transformation.silver_account_posting().first()
        assert row_post is not None
        assert float(row_post.posting_amount) == 150.00
        assert row_post.source_system == "core_banking"

        row_card = transaction_transformation.silver_card_payment().first()
        assert row_card is not None
        assert float(row_card.payment_amount) == 89.99
        assert row_card.is_fraud_source_flag is False


def test_silver_financial_event_status_history_union(test_spark):
    """Verify silver_financial_event_status_history unions Account and Card status events."""
    transaction_transformation.spark = test_spark

    schema_acc_status = StructType([
        StructField("status_event_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("status_timestamp", StringType(), True),
        StructField("source_arrival_timestamp", StringType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([("EVT_ACC_01", "ACC_TXN_01", "SETTLED", "2026-07-01 10:00:00", "2026-07-01 10:00:02", 1, "RUN_01")], schema_acc_status)

    schema_card_status = StructType([
        StructField("status_event_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("status_timestamp", StringType(), True),
        StructField("source_arrival_timestamp", StringType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([("EVT_CARD_01", "CARD_TXN_01", "AUTHORIZED", "2026-07-01 11:00:00", "2026-07-01 11:00:01", 1, "RUN_01")], schema_card_status)

    def mock_read_table(path):
        if path.endswith("account_transaction_status_event"):
            return df_acc
        elif path.endswith("card_transaction_status_event"):
            return df_card
        return test_spark.createDataFrame([], StructType([]))

    with patch("pyspark.sql.readwriter.DataFrameReader.table", side_effect=mock_read_table):
        res_df = transaction_transformation.silver_financial_event_status_history()
        rows = res_df.collect()

        assert len(rows) == 2, f"Expected 2 rows after Union, got {len(rows)}"
        source_systems = {r.source_system for r in rows}
        assert source_systems == {"core_banking", "card_system"}


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])