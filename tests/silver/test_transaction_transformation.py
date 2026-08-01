"""Additional unit tests for pipeline.silver.transaction_transformation module.

Tests the main silver_financial_event function that unions multiple sources,
and edge cases not covered in the existing test suite.
"""

import os
import sys
from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION & MODULE IMPORT
# ------------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
SEARCH_DIR = CURRENT_DIR
FOUND_SRC = None

while SEARCH_DIR:
    if os.path.exists(os.path.join(SEARCH_DIR, "src", "pipeline")):
        FOUND_SRC = os.path.join(SEARCH_DIR, "src")
        break
    elif os.path.exists(os.path.join(SEARCH_DIR, "pipeline")):
        FOUND_SRC = SEARCH_DIR
        break
    parent = os.path.dirname(SEARCH_DIR)
    if parent == SEARCH_DIR:
        break
    SEARCH_DIR = parent

if FOUND_SRC and FOUND_SRC not in sys.path:
    sys.path.insert(0, FOUND_SRC)

# Import target functions
from pipeline.silver.transaction_transformation import (
    hash_key,
    get_currency_col,
    silver_financial_event
)


# ==============================================================================
# TEST: MAIN FINANCIAL EVENT UNION
# ==============================================================================

def test_silver_financial_event_unions_all_sources(test_spark):
    """Verify silver_financial_event correctly unions account, card, ATM, and gateway events."""
    
    # Mock account transaction source
    schema_acc = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([
        ("TXN_001", 10001, "CUST_001", "2026-01-01T10:00:00", "USD", "RUN_01")
    ], schema_acc)

    # Mock card transaction source
    schema_card = StructType([
        StructField("card_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([
        ("CARD_TXN_001", "CARD_001", "MERCH_001", "2026-01-01T11:00:00", "VND", "RUN_01")
    ], schema_card)

    # Mock ATM log source
    schema_atm = StructType([
        StructField("log_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_atm = test_spark.createDataFrame([
        ("LOG_001", "TXN_002", "CARD_002", "2026-01-01T12:00:00", "VND", "RUN_01")
    ], schema_atm)

    # Mock gateway log source
    schema_gw = StructType([
        StructField("gateway_txn_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("gateway_timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_gw = test_spark.createDataFrame([
        ("GW_001", "TXN_003", "CARD_TXN_002", "MERCH_002", "2026-01-01T13:00:00", "USD", "RUN_01")
    ], schema_gw)

    # Mock spark.read.table to return appropriate dataframes
    def mock_read_table(path):
        if "account_transaction" in path:
            return df_acc
        elif "card_transaction" in path:
            return df_card
        elif "log_atm" in path:
            return df_atm
        elif "payment_gateway_log" in path:
            return df_gw
        return test_spark.createDataFrame([], StructType([]))

    with patch("pipeline.silver.transaction_transformation.spark") as mock_spark:
        mock_spark.read.table.side_effect = mock_read_table

        result_df = silver_financial_event()
        rows = result_df.collect()

        # Verify we have all four event types
        assert len(rows) == 4, "Should contain all four event sources"
        
        event_types = {r.event_type for r in rows}
        expected_types = {"ACCOUNT_POSTING", "CARD_PAYMENT", "ATM_ACTIVITY", "GATEWAY_PAYMENT"}
        assert event_types == expected_types, f"Expected {expected_types}, got {event_types}"

        # Verify each event has required fields
        for row in rows:
            assert len(row.financial_event_key) == 64, "financial_event_key must be SHA-256 (64 chars)"
            assert row.currency in ["USD", "VND"], f"Unexpected currency: {row.currency}"
            assert row.data_quality_status == "VALID"
            assert row.source_system is not None


def test_silver_financial_event_handles_null_currencies(test_spark):
    """Verify silver_financial_event defaults null currencies to VND."""
    
    # Account transaction with null currency
    schema_acc = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([
        ("TXN_001", 10001, "CUST_001", "2026-01-01T10:00:00", None, "RUN_01")
    ], schema_acc)

    # Mock empty dataframes for other sources
    empty_schema = StructType([StructField("dummy", StringType(), True)])
    df_empty = test_spark.createDataFrame([], empty_schema)

    def mock_read_table(path):
        if "account_transaction" in path:
            return df_acc
        # Return empty dataframes with proper schema for union
        return test_spark.createDataFrame([], StructType([
            StructField("financial_event_key", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("account_key", StringType(), True),
            StructField("payment_card_key", StringType(), True),
            StructField("party_key", StringType(), True),
            StructField("merchant_location_key", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("occurred_at", TimestampType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_business_key", StringType(), True),
            StructField("bronze_record_ref", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("data_quality_status", StringType(), True)
        ]))

    with patch("pipeline.silver.transaction_transformation.spark") as mock_spark:
        mock_spark.read.table.side_effect = mock_read_table

        result_df = silver_financial_event()
        row = result_df.first()

        assert row.currency == "VND", "Null currency should default to VND"


def test_silver_financial_event_preserves_source_system_mapping(test_spark):
    """Verify each source maps to the correct source_system identifier."""
    
    # Create minimal test data for each source
    schema_acc = StructType([
        StructField("account_txn_id", StringType(), True),
        StructField("account_id", LongType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_acc = test_spark.createDataFrame([
        ("TXN_001", 10001, "CUST_001", "2026-01-01T10:00:00", "RUN_01")
    ], schema_acc)

    schema_card = StructType([
        StructField("card_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_card = test_spark.createDataFrame([
        ("CARD_001", "CARD_001", "MERCH_001", "2026-01-01T11:00:00", "RUN_01")
    ], schema_card)

    schema_atm = StructType([
        StructField("log_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_atm = test_spark.createDataFrame([
        ("LOG_001", "TXN_002", "CARD_002", "2026-01-01T12:00:00", "RUN_01")
    ], schema_atm)

    schema_gw = StructType([
        StructField("gateway_txn_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("gateway_timestamp", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])
    df_gw = test_spark.createDataFrame([
        ("GW_001", "TXN_003", "CARD_TXN_002", "MERCH_002", "2026-01-01T13:00:00", "RUN_01")
    ], schema_gw)

    def mock_read_table(path):
        if "account_transaction" in path:
            return df_acc
        elif "card_transaction" in path:
            return df_card
        elif "log_atm" in path:
            return df_atm
        elif "payment_gateway_log" in path:
            return df_gw
        return test_spark.createDataFrame([], StructType([]))

    with patch("pipeline.silver.transaction_transformation.spark") as mock_spark:
        mock_spark.read.table.side_effect = mock_read_table

        result_df = silver_financial_event()
        rows = result_df.collect()

        # Verify source system mappings
        source_systems = {r.event_type: r.source_system for r in rows}
        
        assert source_systems["ACCOUNT_POSTING"] == "core_banking"
        assert source_systems["CARD_PAYMENT"] == "card_system"
        assert source_systems["ATM_ACTIVITY"] == "atm_system"
        assert source_systems["GATEWAY_PAYMENT"] == "payment_gateway"


# ==============================================================================
# TEST: EDGE CASES
# ==============================================================================

def test_hash_key_with_null_values(test_spark):
    """Verify hash_key handles null values by coalescing to empty string."""
    df = test_spark.createDataFrame([
        ("core_banking", None, "TXN_001"),
        ("core_banking", "", "TXN_001")
    ], ["system", "event_type", "txn_id"])

    result_df = df.select(hash_key("system", "event_type", "txn_id").alias("key_hash"))
    hashes = [r.key_hash for r in result_df.collect()]

    # Both null and empty string should produce the same hash after coalesce
    assert hashes[0] == hashes[1], "Null and empty string should hash identically"
    assert len(hashes[0]) == 64


def test_get_currency_col_with_mixed_nulls(test_spark):
    """Verify get_currency_col handles mixed null and valid values."""
    df = test_spark.createDataFrame([
        ("USD",),
        (None,),
        ("EUR",),
        (None,)
    ], ["currency"])

    result_df = df.select(get_currency_col(df))
    currencies = [r.currency for r in result_df.collect()]

    assert currencies == ["USD", "VND", "EUR", "VND"], "Null values should default to VND"


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])