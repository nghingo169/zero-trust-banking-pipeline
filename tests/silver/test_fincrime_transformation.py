"""Unit tests for pipeline.silver.fincrime_transformation module.

Tests helper functions (hash_key, get_pipeline_run_id) and table transformation builders
(silver_fraud_alert, silver_investigation_case, silver_aml_case, silver_chargeback,
silver_call_center_contact, etc.) using mock Spark DataFrames.
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

# Import functions under test from fincrime_transformation module
from pipeline.silver.fincrime_transformation import (
    hash_key,
    get_pipeline_run_id,
    silver_financial_event_risk_score,
    silver_fraud_alert,
    silver_investigation_case,
    silver_aml_case,
    silver_sanctions_screening,
    silver_chargeback,
    silver_call_center_contact,
    AES_KEY
)


# ==============================================================================
# SECTION 1: HELPER FUNCTION TESTS
# ==============================================================================

def test_hash_key_generates_valid_sha256(test_spark):
    """Verify hash_key produces deterministic 64-char SHA-256 hashes."""
    df = test_spark.createDataFrame([
        ("fincrime", "fraud_alert", "ALT_1001"),
        ("fincrime", "fraud_alert", "  ALT_1001  ")  # Whitspace test
    ], ["system", "entity", "id"])

    result_df = df.select(hash_key("system", "entity", "id").alias("key_hash"))
    hashes = [r.key_hash for r in result_df.collect()]

    assert len(hashes[0]) == 64, "SHA-256 output must be 64 hexadecimal characters"
    assert hashes[0] == hashes[1], "Trimming whitespace must produce identical hash values"


def test_get_pipeline_run_id_extracts_column(test_spark):
    """Verify get_pipeline_run_id extracts pipeline_run_id column directly when present."""
    df = test_spark.createDataFrame([("RUN_FINCRIME_001",)], ["pipeline_run_id"])
    result_df = df.select(get_pipeline_run_id(df).alias("run_id"))

    assert result_df.first().run_id == "RUN_FINCRIME_001"


# ==============================================================================
# SECTION 2: FRAUD, RISK & CASE INVESTIGATION TRANSFORMATIONS
# ==============================================================================

def test_silver_financial_event_risk_score_transformation(test_spark):
    """Verify silver_financial_event_risk_score casts decimal precision correctly."""
    schema = StructType([
        StructField("score_id", StringType(), True),
        StructField("account_txn_id", StringType(), True),
        StructField("model_score", DoubleType(), True),
        StructField("risk_band", StringType(), True),
        StructField("scored_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("SCORE_01", "TXN_101", 0.8542, "HIGH", "2026-01-15", "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark:
        mock_spark.read.table.return_value = raw_df

        result_df = silver_financial_event_risk_score()
        row = result_df.first()

        assert row.model_score == pytest.approx(0.8542)
        assert row.risk_band == "HIGH"
        assert row.source_system == "fincrime"
        assert row.bronze_record_ref == "account_transaction_risk_score:SCORE_01"


def test_silver_fraud_alert_transformation(test_spark):
    """Verify silver_fraud_alert processes alert_score and created_at timestamps."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("alert_score", DoubleType(), True),
        StructField("alert_status", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("ALT_01", "CARD_FRAUD", 92.50, "OPEN", "2026-02-01T08:30:00", "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark:
        mock_spark.read.table.return_value = raw_df

        result_df = silver_fraud_alert()
        row = result_df.first()

        assert row.alert_score == pytest.approx(92.50)
        assert row.alert_status == "OPEN"
        assert len(row.fraud_alert_key) == 64


def test_silver_investigation_case_transformation(test_spark):
    """Verify silver_investigation_case builds case details and keys correctly."""
    schema = StructType([
        StructField("case_id", StringType(), True),
        StructField("investigation_type", StringType(), True),
        StructField("case_origin", StringType(), True),
        StructField("case_status", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("opened_timestamp", StringType(), True),
        StructField("closed_timestamp", StringType(), True),
        StructField("assigned_analyst_id", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("CASE_100", "AML_SUSPICIOUS", "ALERT_SYSTEM", "IN_PROGRESS", "HIGH", "2026-01-10T09:00:00", None, "ANALYST_07", "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark:
        mock_spark.read.table.return_value = raw_df

        result_df = silver_investigation_case()
        row = result_df.first()

        assert row.case_status == "IN_PROGRESS"
        assert row.assigned_analyst_id == "ANALYST_07"
        assert row.closed_at is None


# ==============================================================================
# SECTION 3: AML, SANCTIONS, CHARGEBACK & CALL CENTER PII MASKING
# ==============================================================================

def test_silver_aml_case_and_sanctions_screening(test_spark):
    """Verify silver_aml_case links to investigation cases and parties."""
    schema = StructType([
        StructField("case_id", StringType(), True),
        StructField("investigation_case_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("case_type", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("opened_date", StringType(), True),
        StructField("closed_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("AML_01", "CASE_100", "CB-1001", "MONEY_LAUNDERING", "HIGH", "2026-01-15", None, "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark:
        mock_spark.read.table.return_value = raw_df

        result_df = silver_aml_case()
        row = result_df.first()

        assert row.source_business_key == "AML_01"
        assert row.opened_date == date(2026, 1, 15)
        assert len(row.party_key) == 64


def test_silver_chargeback_transformation(test_spark):
    """Verify silver_chargeback casts dispute_amount to Decimal(12,2)."""
    schema = StructType([
        StructField("chargeback_id", StringType(), True),
        StructField("card_txn_id", StringType(), True),
        StructField("reason_code", StringType(), True),
        StructField("dispute_amount", DoubleType(), True),
        StructField("filed_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("resolved_date", StringType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("CBK_01", "CARD_TXN_99", "4837", 450.00, "2026-02-01", "OPEN", None, "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark:
        mock_spark.read.table.return_value = raw_df

        result_df = silver_chargeback()
        row = result_df.first()

        assert row.dispute_amount == pytest.approx(450.00)
        assert row.reason_code == "4837"
        assert row.chargeback_status == "OPEN"


def test_silver_call_center_contact_pii_masking_and_encryption(test_spark):
    """Verify silver_call_center_contact applies phone masking, AES encryption, and SHA-256 tokenization."""
    schema = StructType([
        StructField("call_id", StringType(), True),
        StructField("customer_ref", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("caller_phone", StringType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("call_reason", StringType(), True),
        StructField("agent_id", StringType(), True),
        StructField("call_duration_seconds", LongType(), True),
        StructField("pipeline_run_id", StringType(), True)
    ])

    raw_df = test_spark.createDataFrame([
        ("CALL_01", "CB-1001", "CASE_100", "0901234567", "2026-02-01T14:00:00", "DISPUTE", "AGENT_09", 180, "RUN_01")
    ], schema)

    with patch("pipeline.silver.fincrime_transformation.spark") as mock_spark, \
         patch("pipeline.silver.fincrime_transformation.nab_mask_phone") as mock_mask_phone:

        mock_spark.read.table.return_value = raw_df
        mock_mask_phone.return_value = F.lit("090XXXX567")

        result_df = silver_call_center_contact()
        row = result_df.first()

        # Structural assertions
        assert row.call_duration_seconds == 180
        assert row.source_system == "fincrime"

        # PII Security assertions
        assert row.caller_phone_masked == "090XXXX567", "Masking function must be called"
        assert row.caller_phone_encrypted is not None, "AES encrypted phone must not be NULL"
        assert row.caller_phone_encrypted != "0901234567", "Raw phone number must be encrypted"
        assert len(row.caller_phone_token) == 64, "Caller phone token must be a valid SHA-256 hex string"


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])